/* Copyright 2014 UniCredit S.p.A.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package unicredit.spark.hbase

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

import HFileRDDSupport._

trait HFileSupport {

  implicit lazy val cellKeyOrdering = new CellKeyOrdering
  implicit lazy val cellKeyTOrdering = new CellKeyTOrdering

  implicit def toHFileRDDSimple[A: ClassTag](rdd: RDD[(String, Map[String, A])])(implicit writer: Writes[A]): HFileRDDSimple[CellKey, A] =
    new HFileRDDSimple[CellKey, A](rdd, gck[A], kvw[A])

  implicit def toHFileRDDSimpleT[A](rdd: RDD[(String, Map[String, (A, Long)])])(implicit writer: Writes[A]): HFileRDDSimple[CellKeyT, (A, Long)] =
    new HFileRDDSimple[CellKeyT, (A, Long)](rdd, gck[A], kvw[A])

  implicit def toHFileRDDFixed[A: ClassTag](rdd: RDD[(String, Seq[A])])(implicit writer: Writes[A]): HFileRDDFixed[CellKey, A] =
    new HFileRDDFixed[CellKey, A](rdd, gck[A], kvw[A])

  implicit def toHFileRDDFixedT[A](rdd: RDD[(String, Seq[(A, Long)])])(implicit writer: Writes[A]): HFileRDDFixed[CellKeyT, (A, Long)] =
    new HFileRDDFixed[CellKeyT, (A, Long)](rdd, gck[A], kvw[A])

  implicit def toHFileRDD[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: Writes[A]): HFileRDD[CellKey, A] =
    new HFileRDD[CellKey, A](rdd, gck[A], kvw[A])

  implicit def toHFileRDDT[A](rdd: RDD[(String, Map[String, Map[String, (A, Long)]])])(implicit writer: Writes[A]): HFileRDD[CellKeyT, (A, Long)] =
    new HFileRDD[CellKeyT, (A, Long)](rdd, gck[A], kvw[A])

}

private[hbase] object HFileRDDSupport {

  type CellKey = (Array[Byte], Array[Byte], Array[Byte]) // (rowkey, family, qualifier)
  type CellKeyT = (CellKey, Array[Byte]) // (CellKey, timestamp)
  type GetCellKey[C, A] = (CellKey, A) => C
  type KeyValueWrapper[C, A] = (C, A) => (ImmutableBytesWritable, KeyValue)

  // GetCellKey
  def gck[A](c: CellKey, v: A) = c
  def gck[A](c: CellKey, v: (A, Long)) = (c, Bytes.toBytes(v._2))

  // KeyValueWrapper
  def kvw[A](c: CellKey, v: A)(implicit writer: Writes[A]) =
    (new ImmutableBytesWritable(c._1), new KeyValue(c._1, c._2, c._3, writer.write(v)))
  def kvw[A](c: CellKeyT, v: (A, Long))(implicit writer: Writes[A]) =
    (new ImmutableBytesWritable(c._1._1), new KeyValue(c._1._1, c._1._2, c._1._3, v._2, writer.write(v._1)))

  class CellKeyOrdering extends Ordering[CellKey] {
    override def compare(a: CellKey, b: CellKey) = {
      val (ak, af, aq) = a
      val (bk, bf, bq) = b
      // compare keys
      var ord = Bytes.compareTo(ak, bk)
      if (ord != 0) ord
      else {
        // compare families
        ord = Bytes.compareTo(af, bf)
        if (ord != 0) ord
        else Bytes.compareTo(aq, bq) // compare qualifiers
      }
    }
  }

  class CellKeyTOrdering extends Ordering[CellKeyT] {
    val cellKeyOrdering = new CellKeyOrdering
    override def compare(a: CellKeyT, b: CellKeyT) = {
      val (ac, at) = a
      val (bc, bt) = b
      val ord = cellKeyOrdering.compare(ac, bc)
      if (ord != 0) ord
      else {
        // compare timestamps
        // see org.apache.hadoop.hbase.KeyValue.KVComparator.compareTimestamps(long, long)
        val atl = Bytes.toLong(at)
        val btl = Bytes.toLong(bt)
        if (atl < btl) 1
        else if (atl > btl) -1
        else 0
      }
    }
  }
}

sealed abstract class HFileRDDHelper extends Serializable {

  private object HFilePartitioner {
    def apply(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) = {
      if (numFilesPerRegion == 1)
        new SingleHFilePartitioner(splits)
      else {
        val fraction = 1 max numFilesPerRegion min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
        new MultiHFilePartitioner(splits, fraction)
      }
    }
  }

  private abstract class HFilePartitioner extends Partitioner {
    def extractKey(n: Any) = n match {
      case c: CellKey => c._1
      case c: CellKeyT => c._1._1
    }
  }

  private class MultiHFilePartitioner(splits: Array[Array[Byte]], fraction: Int) extends HFilePartitioner {
    override def getPartition(key: Any): Int = {
      val k = extractKey(key)
      val h = (k.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(k, splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  private class SingleHFilePartitioner(splits: Array[Array[Byte]]) extends HFilePartitioner {
    override def getPartition(key: Any): Int = {
      val k = extractKey(key)
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(k, splits(i)) < 0) return i - 1

      splits.length - 1
    }

    override def numPartitions: Int = splits.length
  }

  protected def toHBaseBulk[C: ClassTag, A: ClassTag](rdd: RDD[(C, A)],
    tableName: String, numFilesPerRegion: Int, kv: KeyValueWrapper[C, A])(implicit config: HBaseConfig, ord: Ordering[C]) = {
    val conf = config.get
    val hTable = new HTable(conf, tableName)
    val cf = Bytes.toBytes(cFamily)
    
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))

    HFileOutputFormat2.configureIncrementalLoad(job, hTable)

    // prepare path for HFiles output
    val fs = FileSystem.get(conf)
    val hFilePath = new Path("/tmp", tableName + "_" + UUID.randomUUID())
    fs.makeQualified(hFilePath)

    try {
      rdd
        .repartitionAndSortWithinPartitions(HFilePartitioner(conf, hTable.getStartKeys, numFilesPerRegion))
        .map { case (cell, value) => kv(cell, value) }
        .saveAsNewAPIHadoopFile(hFilePath.toString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)

      // prepare HFiles for incremental load
      // set folders permissions read/write/exec for all
      val rwx = new FsPermission("777")
      def setRecursivePermission(path: Path): Unit = {
        val listFiles = fs.listStatus(path)
        listFiles foreach { f =>
          val p = f.getPath
          fs.setPermission(p, rwx)
          if (f.isDirectory && p.getName != "_tmp") {
            // create a "_tmp" folder that can be used for HFile splitting, so that we can
            // set permissions correctly. This is a workaround for unsecured HBase. It should not
            // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
            // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
            FileSystem.mkdirs(fs, new Path(p, "_tmp"), rwx)
            setRecursivePermission(p)
          }
        }
      }
      setRecursivePermission(hFilePath)

      val lih = new LoadIncrementalHFiles(conf)
      lih.doBulkLoad(hFilePath, hTable)
    } finally {
      fs.deleteOnExit(hFilePath)

      // clean HFileOutputFormat2 stuff
      fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
    }
  }
}

final class HFileRDDSimple[C: ClassTag, A: ClassTag](mapRdd: RDD[(String, Map[String, A])], ck: GetCellKey[C, A], kv: KeyValueWrapper[C, A]) extends HFileRDDHelper {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * Simplified form, where all values are written to the
   * same column family.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, A])]`,
   * where the first value is the rowkey and the second is a map that
   * associates column names to values.
   */
  def toHBaseBulk(tableName: String, family: String, numFilesPerRegion: Int = 1)(implicit config: HBaseConfig, ord: Ordering[C]) = {
    val familyBytes = Bytes.toBytes(family)
    val rdd = mapRdd.flatMap {
      case (k, m) =>
        val keyBytes = Bytes.toBytes(k)
        m map { case (h, v) => (ck((keyBytes, familyBytes, h), v), v) }
    }
    super.toHBaseBulk(rdd, tableName, numFilesPerRegion, kv)
  }
}

final class HFileRDDFixed[C: ClassTag, A: ClassTag](seqRdd: RDD[(String, Seq[A])], ck: GetCellKey[C, A], kv: KeyValueWrapper[C, A]) extends HFileRDDHelper {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * Simplified form, where all values are written to the
   * same column family, and columns are fixed, so that their names can be passed as a sequence.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Seq[A])`,
   * where the first value is the rowkey and the second is a sequence of values to be
   * associated to column names in `headers`.
   */
  def toHBaseBulk(tableName: String, family: String, headers: Seq[String], numFilesPerRegion: Int = 1)(implicit config: HBaseConfig, ord: Ordering[C]) = {
    require(numFilesPerRegion > 0)
    val sc = seqRdd.context
    val headersBytes = sc.broadcast(headers map Bytes.toBytes)
    val familyBytes = Bytes.toBytes(family)
    val rdd = seqRdd.flatMap {
      case (k, v) =>
        val keyBytes = Bytes.toBytes(k)
        (headersBytes.value zip v) map { case (h, v) => (ck((keyBytes, familyBytes, h), v), v) }
    }
    super.toHBaseBulk(rdd, tableName, numFilesPerRegion, kv)
  }
}

final class HFileRDD[C: ClassTag, A: ClassTag](mapRdd: RDD[(String, Map[String, Map[String, A]])], ck: GetCellKey[C, A], kv: KeyValueWrapper[C, A]) extends HFileRDDHelper {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, Map[String, A]])]`,
   * where the first value is the rowkey and the second is a nested map that associates
   * column families and column names to values.
   */
  def toHBaseBulk(tableName: String, numFilesPerRegion: Int = 1)(implicit config: HBaseConfig, ord: Ordering[C]) = {
    require(numFilesPerRegion > 0)
    val rdd = mapRdd.flatMap {
      case (k, m) =>
        val keyBytes = Bytes.toBytes(k)
        for {
          (f, cv) <- m
          familyBytes = Bytes.toBytes(f)
          (c, v) <- cv
        } yield (ck((keyBytes, familyBytes, c), v), v)
    }
    super.toHBaseBulk(rdd, tableName, numFilesPerRegion, kv)
  }
}
