package unicredit.spark

import scala.util.control.Exception.allCatch

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.{ HBaseConfiguration, CellUtil }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithDouble._

trait HBaseReadSupport {
  implicit def toHBaseSC(sc: SparkContext): HBaseSC = new HBaseSC(sc)

  implicit def bytes2string(bytes: Array[Byte]): String = new String(bytes)
  implicit def bytes2json(bytes: Array[Byte]): JValue = parse(new String(bytes))
}

final class HBaseSC(@transient sc: SparkContext) extends Serializable {
  def extract[A](data: Map[String, List[String]], result: Result, interpret: Array[Byte] => A): Map =
    data map {
      case (cf, columns) =>
        val content = columns flatMap { column =>
          val cell = result.getColumnLatestCell(cf.getBytes, column.getBytes)

          allCatch opt { CellUtil.cloneValue(cell) } map { value =>
            column -> interpret(value)
          }
        } toMap

        cf -> content
    }

  def makeConf(config: HBaseConfig, table: String): HBaseConfiguration = {
    val conf = HBaseConfiguration.create()

    conf.setBoolean("hbase.cluster.distributed", true)
    conf.setInt("hbase.client.scanner.caching", 10000)
    conf.set(TableInputFormat.INPUT_TABLE, table)
    config(conf)

    conf
  }

  def hbase[A](table: String, data: Map[String, List[String]])(implicit config: HBaseConfig, interpret: Array[Byte] => A): RDD =

    sc.newAPIHadoopRDD(makeConf(config, table), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
        case (key, row) =>
          Bytes.toString(key.get) -> extract(data, row, interpret)
      }

  def hbaseRaw(table: String)(implicit config: HBaseConfig): RDD =

    sc.newAPIHadoopRDD(makeConf(config, table), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
        case (key, row) =>
          Bytes.toString(key.get) -> row
      }

}
