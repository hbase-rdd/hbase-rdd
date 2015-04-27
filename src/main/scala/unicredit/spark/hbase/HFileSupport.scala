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

import java.text.SimpleDateFormat
import java.util.{ Calendar, TreeSet, UUID }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor, KeyValue, TableName }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partitioner, SparkContext }

import scala.collection.JavaConversions.asScalaSet

trait HFileSupport {

  type KeyValueWrapper[A] = (Array[Byte], Array[Byte], Array[Byte], A) => KeyValue

  implicit def seqRddToHFileRdd[A](rdd: RDD[(String, Seq[A])])(implicit writer: Writes[A]): HFileSeqRDD[A] =
    new HFileSeqRDD[A](rdd, { (k: Array[Byte], cf: Array[Byte], q: Array[Byte], v: A) => new KeyValue(k, cf, q, writer.write(v)) })

  implicit def seqRddToHFileRddT[A](rdd: RDD[(String, Seq[(A, Long)])])(implicit writer: Writes[A]): HFileSeqRDD[(A, Long)] =
    new HFileSeqRDD[(A, Long)](rdd, { (k: Array[Byte], cf: Array[Byte], q: Array[Byte], v: (A, Long)) => new KeyValue(k, cf, q, v._2, writer.write(v._1)) })

  implicit def mapRddToHFileRdd[A](rdd: RDD[(String, Map[String, A])])(implicit writer: Writes[A]): HFileMapRDD[A] =
    new HFileMapRDD[A](rdd, { (k: Array[Byte], cf: Array[Byte], q: Array[Byte], v: A) => new KeyValue(k, cf, q, writer.write(v)) })

  implicit def mapRddToHFileRddT[A](rdd: RDD[(String, Map[String, (A, Long)])])(implicit writer: Writes[A]): HFileMapRDD[(A, Long)] =
    new HFileMapRDD[(A, Long)](rdd, { (k: Array[Byte], cf: Array[Byte], q: Array[Byte], v: (A, Long)) => new KeyValue(k, cf, q, v._2, writer.write(v._1)) })

  /**
   * Checks if table exists, and requires that it contains the desired column family
   *
   * @param tableName name of the table
   * @param cFamily name of the column family
   *
   * @return true if table exists, false otherwise
   */
  def tableExists(tableName: String, cFamily: String)(implicit config: HBaseConfig): Boolean =
    tableExists(new HBaseAdmin(config.get), tableName, cFamily)

  private def tableExists(admin: HBaseAdmin, tableName: String, cFamily: String): Boolean = {
    if (admin.tableExists(tableName)) {
      // check if passed cFamily exists
      val families = admin.getTableDescriptor(tableName.getBytes).getFamiliesKeys
      require(families.contains(cFamily.getBytes), s"Table [$tableName] exists but column family [$cFamily] is missing")
      true
    } else false
  }

  /**
   * Takes a snapshot of the table, the snapshot's name has format "tableName_yyyyMMddHHmmss"
   *
   * @param tableName name of the table
   */
  def snapshot(tableName: String)(implicit config: HBaseConfig): Unit =
    snapshot(new HBaseAdmin(config.get), new HTableDescriptor(TableName.valueOf(tableName)))

  /**
   * Takes a snapshot of the table
   *
   * @param tableName name of the table
   * @param snapShotName name of the snapshot
   */
  def snapshot(tableName: String, snapShotName: String)(implicit config: HBaseConfig): Unit =
    snapshot(new HBaseAdmin(config.get), new HTableDescriptor(TableName.valueOf(tableName)), snapShotName)

  private def snapshot(admin: HBaseAdmin, tableDescriptor: HTableDescriptor): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val suffix = sdf.format(Calendar.getInstance().getTime)
    val tName = tableDescriptor.getTableName
    snapshot(admin, tableDescriptor, s"${tName.getNameAsString}_$suffix")
  }

  private def snapshot(admin: HBaseAdmin, tableDescriptor: HTableDescriptor, snapShotName: String): Unit = {
    val tName = tableDescriptor.getTableName
    admin.snapshot(snapShotName, tName)
  }

  /**
   * Creates a table with a column family and made of one or more regions
   *
   * @param tableName name of the table
   * @param cFamily name of the column family
   * @param splitKeys ordered list of keys that defines region splits
   */
  def createTable(tableName: String, cFamily: String, splitKeys: Seq[String])(implicit config: HBaseConfig): Unit =
    createTable(new HBaseAdmin(config.get), new HTableDescriptor(TableName.valueOf(tableName)), cFamily, splitKeys)

  private def createTable(admin: HBaseAdmin, tableDescriptor: HTableDescriptor, cFamily: String, splitKeys: Seq[String]): Unit = {
    tableDescriptor.addFamily(new HColumnDescriptor(cFamily))
    if (splitKeys.isEmpty)
      admin.createTable(tableDescriptor)
    else {
      val splitKeysBytes = splitKeys.map(_.getBytes).toArray
      admin.createTable(tableDescriptor, splitKeysBytes)
    }
  }

  /**
   * If the table exists, checks whether it contains the desired column family, and return false otherwise.
   * If table does not exist, computes the split keys and creates a table with the split keys and the
   * desired column family.
   *
   * @param sc Spark context needed to compute region splits
   * @param tableName name of the table
   * @param cFamily name of the column family
   * @param filename path of the input tsv file
   * @param regionSize desired size of table regions, expressed as a number followed by B, K, M, G (e.g. "10G")
   * @param header name of the row key header, in case the tsv contains headers, it can be null otherwise
   *               the row key must be the first field in a tsv line
   * @param takeSnapshot if true and the table exists, take a snapshot of the table
   *
   * @return true if table exists or it is created and has the required column family, false otherwise
   */
  @deprecated("too specific, use the other prepareTable instead", "0.4.3")
  def prepareTable(sc: SparkContext,
    tableName: String,
    cFamily: String,
    filename: String,
    regionSize: String,
    header: String,
    takeSnapshot: Boolean)(implicit config: HBaseConfig): Boolean =
    prepareTable(config.get, tableName, cFamily, takeSnapshot, computeSplits(sc, filename, regionSize, header))

  /**
   * If the table exists, checks whether it contains the desired column family, and return false otherwise.
   * If table does not exist, computes the split keys and creates a table with the split keys and the
   * desired column family.
   *
   * @param tableName name of the table
   * @param cFamily name of the column family
   * @param keys RDD containing all the row keys, used to compute split keys in case the table does not exist,
   *             not used otherwise
   * @param splitsCount desired number of splits for the table, not used if table exists
   * @param takeSnapshot if true and the table exists, take a snapshot of the table
   *
   * @return true if table exists or it is created and has the required column family, false otherwise
   */
  def prepareTable(tableName: String,
    cFamily: String,
    keys: RDD[String],
    splitsCount: Int,
    takeSnapshot: Boolean)(implicit config: HBaseConfig): Boolean =
    prepareTable(config.get, tableName, cFamily, takeSnapshot, computeSplits(keys, splitsCount))

  private def prepareTable(conf: Configuration,
    tableName: String,
    cFamily: String,
    takeSnapshot: Boolean,
    computeSplits: => Seq[String]): Boolean = {
    val admin = new HBaseAdmin(conf)
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    if (tableExists(admin, tableName, cFamily)) {
      if (takeSnapshot) snapshot(admin, tableDescriptor)
    } else {
      createTable(admin, tableDescriptor, cFamily, computeSplits)
    }
    true
  }

  def computeSplits(rdd: RDD[String], splitsCount: Int): Seq[String] = {
    rdd.sortBy(s => s, numPartitions = splitsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }

  @deprecated("too specific, use the other computeSplits instead", "0.4.3")
  def computeSplits(sc: SparkContext, filename: String, regionSizeStr: String, header: String)(implicit config: HBaseConfig): Seq[String] = {

    def computeSize(sizeString: String) = {
      val sizeReg = """^([0-9]+)(B|K|M|G)?$""".r
      sizeReg findFirstMatchIn sizeString match {
        case Some(m) =>
          val num = m.group(1).toLong
          m.group(2) match {
            case "B" => num
            case "K" => num * 1024
            case "M" => num * 1024 * 1024
            case "G" => num * 1024 * 1024 * 1024
            case _ => num
          }
        case None => throw new Exception
      }
    }

    val file = new Path(filename)
    val fs = file.getFileSystem(config.get)
    val fileLength = fs.getContentSummary(file).getLength

    val regionSize = computeSize(regionSizeStr)
    val splitsCount = fileLength / regionSize + 1

    val input = sc.textFile(filename)
      .map { line => line.split("\t").head }

    val inputNoHeader = if (header != null) input.filter(_ != header) else input
    val splits = computeSplits(inputNoHeader, splitsCount.toInt)

    splits
  }
}

case class KeyDuplicatedException(key: String) extends Exception(f"rowkey [$key] is not unique")

sealed abstract class HFileRDD extends Serializable {

  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
    val fraction = 1 max numFilesPerRegion min conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)

    override def getPartition(key: Any): Int = {
      def bytes(n: Any) = n match { case s: String => Bytes.toBytes(s) }

      val h = (key.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  protected def loadtohbase[A](rdd: RDD[(String, Map[String, A])],
    tableName: String, cFamily: String, numFilesPerRegion: Int,
    kv: KeyValueWrapper[A])(implicit config: HBaseConfig) = {
    def bytes(s: String) = Bytes.toBytes(s)

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
        .partitionBy(new HFilePartitioner(conf, hTable.getStartKeys, numFilesPerRegion))
        .mapPartitions({ p =>
          p.toSeq.sortWith {
            (r1, r2) =>
              val ord = Bytes.compareTo(bytes(r1._1), bytes(r2._1))
              if (ord == 0) throw KeyDuplicatedException(r1._1)
              ord < 0
          }.toIterator
        }, true)
        .flatMap {
          case (key, columns) =>
            val hKey = new ImmutableBytesWritable()
            hKey.set(bytes(key))
            val kvs = new TreeSet[KeyValue](KeyValue.COMPARATOR)
            for ((q, value) <- columns) kvs.add(kv(hKey.get(), cf, Bytes.toBytes(q), value))
            kvs.toSeq map (kv => (hKey, kv))
        }
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

final class HFileSeqRDD[A](seqRdd: RDD[(String, Seq[A])], kv: KeyValueWrapper[A]) extends HFileRDD {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Seq[A])`,
   * where the first value is the rowkey and the second is a sequence of values to be
   * associated to column names in `headers`.
   */
  def loadtohbase(tableName: String, cFamily: String, headers: Seq[String], numFilesPerRegion: Int = 1)(implicit config: HBaseConfig) = {
    val sc = seqRdd.context
    val headersBytes = sc.broadcast(headers)
    val mapRdd = seqRdd.mapValues(v => Map(headersBytes.value zip v: _*))
    super.loadtohbase(mapRdd, tableName, cFamily, numFilesPerRegion, kv)
  }
}

final class HFileMapRDD[A](mapRdd: RDD[(String, Map[String, A])], kv: KeyValueWrapper[A]) extends HFileRDD {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, A])]`,
   * where the first value is the rowkey and the second is a map that
   * associates column names to values.
   */
  def loadtohbase(tableName: String, cFamily: String, numFilesPerRegion: Int = 1)(implicit config: HBaseConfig) = {
    super.loadtohbase(mapRdd, tableName, cFamily, numFilesPerRegion, kv)
  }
}
