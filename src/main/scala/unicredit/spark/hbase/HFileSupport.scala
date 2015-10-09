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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Table, Connection, RegionLocator, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import HFileMethods._

trait HFileSupport {

  implicit lazy val cellKeyOrdering = new CellKeyOrdering
  implicit lazy val cellKeyTSOrdering = new CellKeyTSOrdering

  implicit def toHFileRDDSimple[A: ClassTag](rdd: RDD[(String, Map[String, A])])(implicit writer: Writes[A]): HFileRDDSimple[CellKey, A, A] =
    new HFileRDDSimple[CellKey, A, A](rdd, gc[A], kvf[A])

  implicit def toHFileRDDSimpleTS[A: ClassTag](rdd: RDD[(String, Map[String, (A, Long)])])(implicit writer: Writes[A]): HFileRDDSimple[CellKeyTS, (A, Long), A] =
    new HFileRDDSimple[CellKeyTS, (A, Long), A](rdd, gc[A], kvft[A])

  implicit def toHFileRDDFixed[A: ClassTag](rdd: RDD[(String, Seq[A])])(implicit writer: Writes[A]): HFileRDDFixed[CellKey, A, A] =
    new HFileRDDFixed[CellKey, A, A](rdd, gc[A], kvf[A])

  implicit def toHFileRDDFixedTS[A: ClassTag](rdd: RDD[(String, Seq[(A, Long)])])(implicit writer: Writes[A]): HFileRDDFixed[CellKeyTS, (A, Long), A] =
    new HFileRDDFixed[CellKeyTS, (A, Long), A](rdd, gc[A], kvft[A])

  implicit def toHFileRDD[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: Writes[A]): HFileRDD[CellKey, A, A] =
    new HFileRDD[CellKey, A, A](rdd, gc[A], kvf[A])

  implicit def toHFileRDDTS[A: ClassTag](rdd: RDD[(String, Map[String, Map[String, (A, Long)]])])(implicit writer: Writes[A]): HFileRDD[CellKeyTS, (A, Long), A] =
    new HFileRDD[CellKeyTS, (A, Long), A](rdd, gc[A], kvft[A])

}

private[hbase] object HFileMethods {

  type CellKey = (Array[Byte], Array[Byte]) // (key, qualifier)
  type CellKeyTS = (CellKey, Array[Byte]) // (CellKey, timestamp)

  type GetCellKey[C, A, V] = (CellKey, A) => (C, V)
  type KeyValueWrapper[C, V] = (C, V) => (ImmutableBytesWritable, KeyValue)
  type KeyValueWrapperF[C, V] = (Array[Byte]) => KeyValueWrapper[C, V]

  // GetCellKey
  def gc[A](c: CellKey, v: A) = (c, v)
  def gc[A](c: CellKey, v: (A, Long)) = ((c, Bytes.toBytes(v._2)), v._1)

  // KeyValueWrapperF
  def kvf[A](f: Array[Byte])(c: CellKey, v: A)(implicit writer: Writes[A]) =
    (new ImmutableBytesWritable(c._1), new KeyValue(c._1, f, c._2, writer.write(v)))
  def kvft[A](f: Array[Byte])(c: CellKeyTS, v: A)(implicit writer: Writes[A]) =
    (new ImmutableBytesWritable(c._1._1), new KeyValue(c._1._1, f, c._1._2, Bytes.toLong(c._2), writer.write(v)))

  class CellKeyOrdering extends Ordering[CellKey] {
    override def compare(a: CellKey, b: CellKey) = {
      val (ak, aq) = a
      val (bk, bq) = b
      // compare keys
      val ord = Bytes.compareTo(ak, bk)
      if (ord != 0) ord
      else Bytes.compareTo(aq, bq) // compare qualifiers
    }
  }

  class CellKeyTSOrdering extends Ordering[CellKeyTS] {
    val cellKeyOrdering = new CellKeyOrdering
    override def compare(a: CellKeyTS, b: CellKeyTS) = {
      val (ac, at) = a
      val (bc, bt) = b
      val ord = cellKeyOrdering.compare(ac, bc)
      if (ord != 0) ord
      else {
        // see org.apache.hadoop.hbase.KeyValue.KVComparator.compareTimestamps(long, long)
        val al = Bytes.toLong(at)
        val bl = Bytes.toLong(bt)
        if (al < bl) 1
        else if (al > bl) -1
        else 0
      }
    }
  }
}

sealed abstract class HFileRDDHelper extends Serializable {

  private object HFilePartitioner {
    def apply(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegionPerFamily: Int) = {
      if (numFilesPerRegionPerFamily == 1)
        new SingleHFilePartitioner(splits)
      else {
        val fraction = 1 max numFilesPerRegionPerFamily min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
        new MultiHFilePartitioner(splits, fraction)
      }
    }
  }

  protected abstract class HFilePartitioner extends Partitioner {
    def extractKey(n: Any) = n match {
      case (k: Array[Byte], _) => k // CellKey
      case ((k: Array[Byte], _), _) => k //CellKeyTS
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

  protected def getPartitioner(regionLocator: RegionLocator, numFilesPerRegionPerFamily: Int)(implicit config: HBaseConfig) =
    HFilePartitioner(config.get, regionLocator.getStartKeys, numFilesPerRegionPerFamily)

  protected def getPartitionedRdd[C: ClassTag, A: ClassTag](rdd: RDD[(C, A)], kv: KeyValueWrapper[C, A], partitioner: HFilePartitioner)(implicit ord: Ordering[C]) = {
    rdd
      .repartitionAndSortWithinPartitions(partitioner)
      .map { case (cell, value) => kv(cell, value) }
  }

  protected def saveAsHFile(rdd: RDD[(ImmutableBytesWritable, KeyValue)], table: Table, regionLocator: RegionLocator, connection: Connection)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))

    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    // prepare path for HFiles output
    val fs = FileSystem.get(conf)
    val hFilePath = new Path("/tmp", table.getName.getNameAsString + "_" + UUID.randomUUID())
    fs.makeQualified(hFilePath)

    try {
      rdd
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
      lih.doBulkLoad(hFilePath, connection.getAdmin, table, regionLocator)
    } finally {
      connection.close()

      fs.deleteOnExit(hFilePath)

      // clean HFileOutputFormat2 stuff
      fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
    }
  }
}

final class HFileRDDSimple[C: ClassTag, A: ClassTag, V: ClassTag](mapRdd: RDD[(String, Map[String, A])], ck: GetCellKey[C, A, V], kvf: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
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
  def toHBaseBulk(tableNameStr: String, family: String, numFilesPerRegionPerFamily: Int = 1)(implicit config: HBaseConfig, ord: Ordering[C]) = {
    require(numFilesPerRegionPerFamily > 0)

    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdd = mapRdd.flatMap {
      case (k, m) =>
        val keyBytes = Bytes.toBytes(k)
        m map { case (h, v) => ck((keyBytes, h), v) }
    }

    saveAsHFile(getPartitionedRdd(rdd, kvf(family), partitioner), table, regionLocator, connection)
  }
}

final class HFileRDDFixed[C: ClassTag, A: ClassTag, V: ClassTag](seqRdd: RDD[(String, Seq[A])], ck: GetCellKey[C, A, V], kvf: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
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
  def toHBaseBulk(tableNameStr: String, family: String, headers: Seq[String], numFilesPerRegionPerFamily: Int = 1)(implicit config: HBaseConfig, ord: Ordering[C]) = {
    require(numFilesPerRegionPerFamily > 0)

    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val sc = seqRdd.context
    val headersBytes = sc.broadcast(headers map Bytes.toBytes)
    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdd = seqRdd.flatMap {
      case (k, v) =>
        val keyBytes = Bytes.toBytes(k)
        (headersBytes.value zip v) map { case (h, v) => ck((keyBytes, h), v) }
    }

    saveAsHFile(getPartitionedRdd(rdd, kvf(family), partitioner), table, regionLocator, connection)
  }
}

final class HFileRDD[C: ClassTag, A: ClassTag, V: ClassTag](mapRdd: RDD[(String, Map[String, Map[String, A]])], ck: GetCellKey[C, A, V], kv: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, Map[String, A]])]`,
   * where the first value is the rowkey and the second is a nested map that associates
   * column families and column names to values.
   */
  def toHBaseBulk(tableNameStr: String, numFilesPerRegionPerFamily: Int = 1)(implicit config: HBaseConfig, ord: Ordering[C]) = {
    require(numFilesPerRegionPerFamily > 0)

    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val families = table.getTableDescriptor.getFamiliesKeys map Bytes.toString
    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdds = for {
      f <- families
      rdd = mapRdd
        .collect { case (k, m) if m.contains(f) => (Bytes.toBytes(k), m(f)) }
        .flatMap {
          case (k, m) =>
            m map { case (h, v) => ck((k, h), v) }
        }
    } yield getPartitionedRdd(rdd, kv(f), partitioner)

    saveAsHFile(rdds.reduce(_ ++ _), table, regionLocator, connection)
  }
}
