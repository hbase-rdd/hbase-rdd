package unicredit.spark.hbase

import org.apache.hadoop.hbase.client.{ Put, HBaseAdmin }
import org.apache.hadoop.hbase.{ HTableDescriptor, HColumnDescriptor, TableName }
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


/**
 * Adds implicit methods to RDD[(String, Map[String, A])] to write
 * to HBase sources.
 */
trait HBaseWriteSupport {
  implicit def toHBaseRDD[A](rdd: RDD[(String, Map[String, A])])
    (implicit writer: Writes[A]) = new HBaseRDD(rdd, writer)

  implicit val stringWriter = new Writes[String] {
    def write(data: String) = data.getBytes
  }

  implicit val jsonWriter = new Writes[JValue] {
    def write(data: JValue) = compact(data).getBytes
  }
}

final class HBaseRDD[A](val rdd: RDD[(String, Map[String, A])], val writer: Writes[A]) extends Serializable {
  private def convert(id: String, values: Map[String, A], family: String) = {
    def bytes(s: String) = Bytes.toBytes(s)

    val put = new Put(bytes(id))
    for ((key, value) <- values) {
      put.add(bytes(family), bytes(key), writer.write(value))
    }
    (new ImmutableBytesWritable, put)
  }

  private def createTable(table: String, family: String, admin: HBaseAdmin) = {
    if (!admin.isTableAvailable(table)) {
      val tableName = TableName.valueOf(table)
      val tableDescriptor = new HTableDescriptor(tableName)

      tableDescriptor.addFamily(new HColumnDescriptor(family))
      admin.createTable(tableDescriptor)
    }
  }

  def toHBase(table: String, family: String)(implicit config: HBaseConfig) = {
    val conf = config.get

    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    createTable(table, family, new HBaseAdmin(conf))

    val jobConf = new JobConf(conf, getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    rdd.map({ case (k, v) => convert(k, v, family) }).saveAsHadoopDataset(jobConf)
  }
}