package unicredit.spark.hbase

import scala.util.control.Exception.allCatch
import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithDouble._


/**
 * Adds implicit methods to SparkContext to read
 * from HBase sources.
 */
trait HBaseReadSupport {
  implicit def toHBaseSC(sc: SparkContext) = new HBaseSC(sc)

  implicit val stringReader = new Reads[String] {
    def read(data: Array[Byte]) = new String(data)
  }

  implicit val jsonReader = new Reads[JValue] {
    def read(data: Array[Byte]) = parse(new String(data))
  }
}

final class HBaseSC(@transient sc: SparkContext) extends Serializable {
  private def extract[A](data: Map[String, Set[String]], result: Result, reader: Reads[A]) =
    data map {
      case (cf, columns) =>
        val content = columns flatMap { column =>
          val cell = result.getColumnLatestCell(cf.getBytes, column.getBytes)

          allCatch opt { CellUtil.cloneValue(cell) } map { value =>
            column -> reader.read(value)
          }
        } toMap

        cf -> content
    }

  private def extractRow[A](data: Set[String], result: Result, reader: Reads[A]) =
    result.listCells groupBy { cell =>
      new String(CellUtil.cloneFamily(cell))
    } filterKeys(data.contains) mapValues { cells =>
      cells map { cell =>
        val column = CellUtil.cloneQualifier(cell)
        val value = CellUtil.cloneValue(cell)

        column -> reader.read(value)
      }
    }

  private def makeConf(config: HBaseConfig, table: String) = {
    val conf = config.get

    conf.set(TableInputFormat.INPUT_TABLE, table)
    conf
  }

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   *
   * Returns an `RDD[(String, Map[String, Map[String, A]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associated column family and column to
   * the value. Columns which are not found are omitted from the map.
   */
  def hbase[A](table: String, data: Map[String, Set[String]])
    (implicit config: HBaseConfig, reader: Reads[A]) =

      sc.newAPIHadoopRDD(makeConf(config, table), classOf[TableInputFormat],
        classOf[ImmutableBytesWritable], classOf[Result]) map {
          case (key, row) =>
            Bytes.toString(key.get) -> extract(data, row, reader)
        }

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * colum families, which are read in full.
   *
   * Returns an `RDD[(String, Map[String, Map[String, A]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associated column family and column to
   * the value.
   */
  def hbaseFull[A](table: String, data: Set[String])
    (implicit config: HBaseConfig, reader: Reads[A]) =

      sc.newAPIHadoopRDD(makeConf(config, table), classOf[TableInputFormat],
        classOf[ImmutableBytesWritable], classOf[Result]) map {
          case (key, row) =>
            Bytes.toString(key.get) -> extractRow(data, row, reader)
        }

  /**
   * Provides an RDD of HBase rows, without interpreting the content
   * of the rows.
   *
   * Returns an `RDD[(String, Result)]`, where the first element is the
   * rowkey and the second element is an instance of
   * `org.apache.hadoop.hbase.client.Result`.
   *
   * The client can then use the full HBase API to process the result.
   */
  def hbaseRaw(table: String)(implicit config: HBaseConfig) =

    sc.newAPIHadoopRDD(makeConf(config, table), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
        case (key, row) =>
          Bytes.toString(key.get) -> row
      }

}