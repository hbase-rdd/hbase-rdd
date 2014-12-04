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

package unicredit.spark.hbase.hfile

import java.text.SimpleDateFormat
import java.util.{ Calendar, TreeSet, UUID }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ HFileOutputFormat2, LoadIncrementalHFiles }
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ TableName, HColumnDescriptor, HTableDescriptor, KeyValue }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.SparkContext.{ rddToOrderedRDDFunctions, rddToPairRDDFunctions }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partitioner, SparkConf, SparkContext }
import unicredit.spark.hbase.HBaseConfig

import scala.collection.JavaConversions.asScalaSet
import scala.reflect.ClassTag

object HFile {
  implicit def seqRddToHFileRdd[K: ClassTag, V](rdd: RDD[(K, Seq[V])]): HFileSeqRDD[K, V] = new HFileSeqRDD[K, V](rdd)

  implicit def mapRddToHFileRdd[K: ClassTag, C, V](rdd: RDD[(K, Map[C, V])]): HFileMapRDD[K, C, V] = new HFileMapRDD[K, C, V](rdd)

  /**
   * If the table exists, checks whether it contains the desired column family, and return false otherwise.
   * If table does not exist, computes the split keys and creates a table with the split keys and the
   * desired column family.
   *
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
  def prepareTable(tableName: String,
    cFamily: String,
    filename: String,
    regionSize: String,
    header: String,
    takeSnapshot: Boolean)(implicit config: HBaseConfig): Boolean =
    prepareTable(config.get, tableName, cFamily, takeSnapshot, computeSplits(config.get, filename, regionSize, header))

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
    val hBaseAdmin = new HBaseAdmin(conf)

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    if (hBaseAdmin.tableExists(tableName)) {
      // check if passed cFamily exists
      val families = hBaseAdmin.getTableDescriptor(tableName.getBytes).getFamiliesKeys
      if (!families.contains(cFamily.getBytes)) {
        println("\nTable [" + tableName + "] exists but column family [" + cFamily + "] is missing")
        print("column families found:")
        for (family <- families)
          print(" " + new String(family))
        println()
        return false
      }

      if (takeSnapshot) {
        val tName = tableDescriptor.getTableName
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        val suffix = sdf.format(Calendar.getInstance().getTime)
        hBaseAdmin.snapshot(tableName + "_" + suffix, tName)
      }
    } else {
      tableDescriptor.addFamily(new HColumnDescriptor(cFamily))

      val splitKeys = computeSplits
      if (splitKeys.isEmpty)
        hBaseAdmin.createTable(tableDescriptor)
      else {
        val splitKeysBytes = splitKeys.map(_.getBytes).toArray
        hBaseAdmin.createTable(tableDescriptor, splitKeysBytes)
      }
    }
    true
  }

  private def computeSplits(rdd: RDD[String], splitsCount: Int): Seq[String] = {
    rdd.map((_, 1))
      .sortByKey(numPartitions = splitsCount)
      .mapPartitions(_.take(1))
      .map(_._1)
      .collect().toList.tail
  }

  private def computeSplits(conf: Configuration, filename: String, regionSizeStr: String, header: String): Seq[String] = {

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
    val fs = file.getFileSystem(conf)
    val fileLength = fs.getContentSummary(file).getLength

    val regionSize = computeSize(regionSizeStr)
    val splitsCount = fileLength / regionSize + 1

    val sparkConf = new SparkConf().setAppName(this.getClass.getName.split('$')(0) + ".computeSplits")
    val sc = new SparkContext(sparkConf)

    val input = sc.textFile(filename)
      .map { line => line.split("\t").head }

    val inputNoHeader = if (header != null) input.filter(_ != header) else input
    val splits = computeSplits(inputNoHeader, splitsCount.toInt)

    sc.stop()

    splits
  }
}

sealed abstract class HFileRDD extends Serializable {

  implicit def ord: Ordering[Array[Byte]] = new Ordering[Array[Byte]] {
    def compare(x: Array[Byte], y: Array[Byte]): Int = Bytes.compareTo(x, y)
  }

  implicit def toBytes(n: Any): Array[Byte] = n match {
    case b: Boolean => Bytes.toBytes(b)
    case d: Double => Bytes.toBytes(d)
    case f: Float => Bytes.toBytes(f)
    case i: Int => Bytes.toBytes(i)
    case l: Long => Bytes.toBytes(l)
    case s: Short => Bytes.toBytes(s)
    case s: String => Bytes.toBytes(s)
    case a: Array[Byte] => a
  }

  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]]) extends Partitioner {
    val fraction = conf.getInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)

    override def getPartition(key: Any): Int = {
      val h = (key.hashCode() & 0x7fffffff) % fraction
      val k = toBytes(key)
      for (i <- 1 until splits.length)
        if (ord.compare(k, splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  protected def loadToHBase[K: ClassTag, C, V](rdd: RDD[(K, Seq[(C, V)])], tableName: String, cFamilyStr: String)(implicit config: HBaseConfig, ord: Ordering[K]) = {
    val conf = config.get
    val hTable = new HTable(conf, tableName)
    val cFamily = cFamilyStr.getBytes

    val job = new Job(conf, this.getClass.getName.split('$')(0))

    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoad(job, hTable)

    // prepare path for HFiles output
    val fs = FileSystem.get(conf)
    val hFilePath = new Path("/tmp", tableName + "_" + UUID.randomUUID())
    fs.makeQualified(hFilePath)

    rdd
      .partitionBy(new HFilePartitioner(conf, hTable.getStartKeys))
      .mapPartitions({ p => p.toSeq.sortBy(_._1).toIterator }, true)
      .flatMap {
        case (key, columns) =>
          val hKey = new ImmutableBytesWritable()
          hKey.set(key)
          val kvs = new TreeSet[KeyValue](KeyValue.COMPARATOR)
          for ((hb, v) <- columns) kvs.add(new KeyValue(hKey.get(), cFamily, hb, v))
          kvs.toSeq map (kv => (hKey, kv))
      }
      .saveAsNewAPIHadoopFile(hFilePath.toString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)

    // prepare HFiles for incremental load
    // set folders permissions read/write/exec for all
    val rwx = new FsPermission("777")
    val listFiles = fs.listStatus(hFilePath)
    listFiles foreach { f =>
      fs.setPermission(f.getPath, rwx)
      // create a "_tmp" folder that can be used for HFile splitting, so that we can
      // set permissions correctly. This is a workaround for unsecured HBase. It should not
      // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
      // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
      if (f.isDir) FileSystem.mkdirs(fs, new Path(f.getPath, "_tmp"), rwx)
    }
    fs.deleteOnExit(hFilePath)

    // clean HFileOutputFormat2 stuff
    fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))

    val lih = new LoadIncrementalHFiles(conf)
    lih.doBulkLoad(hFilePath, hTable)
  }
}

final class HFileSeqRDD[K: ClassTag, V](seqRdd: RDD[(K, Seq[V])]) extends HFileRDD {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Seq[V])`,
   * where the first value is the rowkey and the second is a sequence of values to be
   * associated to column names in `headers`.
   */
  def loadToHBase(tableName: String, cFamilyStr: String, headers: Seq[String])(implicit config: HBaseConfig, ord: Ordering[K]) = {
    val sc = seqRdd.context
    val headersBytes = sc.broadcast(headers map (_.getBytes))
    val rdd = seqRdd.map { case (k, v) => (k, headersBytes.value zip v) }
    super.loadToHBase(rdd, tableName, cFamilyStr)
  }
}

final class HFileMapRDD[K: ClassTag, C, V](mapRdd: RDD[(K, Map[C, V])]) extends HFileRDD {
  /**
   * Load the underlying RDD to HBase, using HFiles.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Map[C, V])]`,
   * where the first value is the rowkey and the second is a map that
   * associates column names to values.
   */
  def loadToHBaseTable(tableName: String, cFamilyStr: String)(implicit config: HBaseConfig, ord: Ordering[K]) = {
    val rdd = mapRdd.map { case (k, cv) => (k, cv.toSeq) }
    super.loadToHBase(rdd, tableName, cFamilyStr)
  }
}