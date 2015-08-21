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

  implicit lazy val cellKQOrdering = new CellKQOrdering
  implicit lazy val cellKQTOrdering = new CellKQTOrdering
  implicit lazy val cellKFQOrdering = new CellKFQOrdering
  implicit lazy val cellKFQTOrdering = new CellKFQTOrdering

  implicit def toHFileRDDSimple[A: ClassTag](rdd: RDD[(String, Map[String, A])])(implicit writer: Writes[A]): HFileRDDSimple[CellKQ, A, A] =
    new HFileRDDSimple[CellKQ, A, A](rdd, gc[A], kvwf[A])

  implicit def toHFileRDDSimpleT[A: ClassTag](rdd: RDD[(String, Map[String, (A, Long)])])(implicit writer: Writes[A]): HFileRDDSimple[CellKQT, (A, Long), A] =
    new HFileRDDSimple[CellKQT, (A, Long), A](rdd, gc[A], kvwft[A])

  implicit def toHFileRDDFixed[A: ClassTag](rdd: RDD[(String, Seq[A])])(implicit writer: Writes[A]): HFileRDDFixed[CellKQ, A, A] =
    new HFileRDDFixed[CellKQ, A, A](rdd, gc[A], kvwf[A])

  implicit def toHFileRDDFixedT[A: ClassTag](rdd: RDD[(String, Seq[(A, Long)])])(implicit writer: Writes[A]): HFileRDDFixed[CellKQT, (A, Long), A] =
    new HFileRDDFixed[CellKQT, (A, Long), A](rdd, gc[A], kvwft[A])

  implicit def toHFileRDD[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: Writes[A]): HFileRDD[CellKFQ, A, A] =
    new HFileRDD[CellKFQ, A, A](rdd, gc[A], kvw[A])

  implicit def toHFileRDDT[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, (A, Long)]])])(implicit writer: Writes[A]): HFileRDD[CellKFQT, (A, Long), A] =
    new HFileRDD[CellKFQT, (A, Long), A](rdd, gc[A], kvw[A])

}

private[hbase] object HFileRDDSupport {

  type CellKQ = (Array[Byte], Array[Byte]) // (key, qualifier)
  type CellKQT = (CellKQ, Array[Byte]) // (CellKQ, timestamp)
  type CellKFQ = (Array[Byte], Array[Byte], Array[Byte]) // (key, family, qualifier)
  type CellKFQT = (CellKFQ, Array[Byte]) // (CellKFQ, timestamp)
  type GetCellKQ[C, A, V] = (CellKQ, A) => (C, V)
  type GetCellKFQ[C, A, V] = (CellKFQ, A) => (C, V)
  type KeyValueWrapper[C, V] = (C, V) => (ImmutableBytesWritable, KeyValue)
  type KeyValueWrapperF[C, V] = (Array[Byte]) => KeyValueWrapper[C, V]

  // GetCellKQ
  def gc[A](c: CellKQ, v: A) = (c, v)
  def gc[A](c: CellKQ, v: (A, Long)) = ((c, Bytes.toBytes(v._2)), v._1)

  // GetCellKFQ
  def gc[A](c: CellKFQ, v: A) = (c, v)
  def gc[A](c: CellKFQ, v: (A, Long)) = ((c, Bytes.toBytes(v._2)), v._1)

  // KeyValueWrapperF
  def kvwf[A](f: Array[Byte])(c: CellKQ, v: A)(implicit writer: Writes[A]) =
    (new ImmutableBytesWritable(c._1), new KeyValue(c._1, f, c._2, writer.write(v)))
  def kvwft[A](f: Array[Byte])(c: CellKQT, v: A)(implicit writer: Writes[A]) =
    (new ImmutableBytesWritable(c._1._1), new KeyValue(c._1._1, f, c._1._2, Bytes.toLong(c._2), writer.write(v)))

  // KeyValueWrapper
  def kvw[V](c: CellKFQ, v: V)(implicit writer: Writes[V]) =
    (new ImmutableBytesWritable(c._1), new KeyValue(c._1, c._2, c._3, writer.write(v)))
  def kvw[V](c: CellKFQT, v: V)(implicit writer: Writes[V]) =
    (new ImmutableBytesWritable(c._1._1), new KeyValue(c._1._1, c._1._2, c._1._3, Bytes.toLong(c._2), writer.write(v)))

  class CellKQOrdering extends Ordering[CellKQ] {
    override def compare(a: CellKQ, b: CellKQ) = {
      val (ak, aq) = a
      val (bk, bq) = b
      // compare keys
      val ord = Bytes.compareTo(ak, bk)
      if (ord != 0) ord
      else Bytes.compareTo(aq, bq) // compare qualifiers
    }
  }

  class CellKFQOrdering extends Ordering[CellKFQ] {
    override def compare(a: CellKFQ, b: CellKFQ) = {
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

  trait CompareTimestamps {
    def compareTimestamps(a: Array[Byte], b: Array[Byte]) = {
      // compare timestamps
      // see org.apache.hadoop.hbase.KeyValue.KVComparator.compareTimestamps(long, long)
      val al = Bytes.toLong(a)
      val bl = Bytes.toLong(b)
      if (al < bl) 1
      else if (al > bl) -1
      else 0
    }
  }

  class CellKQTOrdering extends Ordering[CellKQT] with CompareTimestamps {
    val cellKQOrdering = new CellKQOrdering
    override def compare(a: CellKQT, b: CellKQT) = {
      val (ac, at) = a
      val (bc, bt) = b
      val ord = cellKQOrdering.compare(ac, bc)
      if (ord != 0) ord
      else compareTimestamps(at, bt)
    }
  }
  class CellKFQTOrdering extends Ordering[CellKFQT] with CompareTimestamps {
    val cellKFQOrdering = new CellKFQOrdering
    override def compare(a: CellKFQT, b: CellKFQT) = {
      val (ac, at) = a
      val (bc, bt) = b
      val ord = cellKFQOrdering.compare(ac, bc)
      if (ord != 0) ord
      else compareTimestamps(at, bt)
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
      case (k: Array[Byte], _) => k // CellKQ
      case ((k: Array[Byte], _), _) => k //CellKQT
      case (k: Array[Byte], _, _) => k // CellKFQ
      case ((k: Array[Byte], _, _), _) => k // CellKFQT
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

final class HFileRDDSimple[C: ClassTag, A: ClassTag, V: ClassTag](mapRdd: RDD[(String, Map[String, A])], ck: GetCellKQ[C, A, V], kvf: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
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
        m map { case (h, v) => ck((keyBytes, h), v) }
    }
    super.toHBaseBulk(rdd, tableName, numFilesPerRegion, kvf(familyBytes))
  }
}

final class HFileRDDFixed[C: ClassTag, A: ClassTag, V: ClassTag](seqRdd: RDD[(String, Seq[A])], ck: GetCellKQ[C, A, V], kvf: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
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
        (headersBytes.value zip v) map { case (h, v) => ck((keyBytes, h), v) }
    }
    super.toHBaseBulk(rdd, tableName, numFilesPerRegion, kvf(familyBytes))
  }
}

final class HFileRDD[C: ClassTag, A: ClassTag, V: ClassTag](mapRdd: RDD[(String, Map[String, Map[String, A]])], ck: GetCellKFQ[C, A, V], kv: KeyValueWrapper[C, V]) extends HFileRDDHelper {
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
        } yield ck((keyBytes, familyBytes, c), v)
    }
    super.toHBaseBulk(rdd, tableName, numFilesPerRegion, kv)
  }
}
