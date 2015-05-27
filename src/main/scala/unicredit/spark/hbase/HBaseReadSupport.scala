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

import org.apache.spark.rdd.RDD

import scala.util.control.Exception.allCatch
import scala.collection.JavaConversions._

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.mapreduce.{ TableInputFormat, IdentityTableMapper, TableMapReduceUtil }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.filter.Filter

import org.apache.hadoop.mapreduce.Job

import org.apache.spark._

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Adds implicit methods to SparkContext to read
 * from HBase sources.
 */
trait HBaseReadSupport {
  implicit def toHBaseSC(sc: SparkContext): HBaseSC = new HBaseSC(sc)

  implicit val byteArrayReader = new Reads[Array[Byte]] {
    def read(data: Array[Byte]) = data
  }

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

          allCatch opt {
            CellUtil.cloneValue(cell)
          } map { value =>
            column -> reader.read(value)
          }
        } toMap

        cf -> content
    }

  private def extractRow[A](data: Set[String], result: Result, reader: Reads[A]) =
    result.listCells groupBy { cell =>
      new String(CellUtil.cloneFamily(cell))
    } filterKeys data.contains mapValues { cells =>
      cells map { cell =>
        val column = new String(CellUtil.cloneQualifier(cell))
        val value = CellUtil.cloneValue(cell)

        column -> reader.read(value)
      } toMap
    }

  private def makeConf(config: HBaseConfig, table: String, columns: Option[String] = None,
                       scan: Scan = new Scan) = {
    val conf = config.get

    if (columns.isDefined)
      conf.set(TableInputFormat.SCAN_COLUMNS, columns.get)

    val job = Job.getInstance(conf)
    TableMapReduceUtil.initTableMapperJob(table, scan, classOf[IdentityTableMapper], null, null, job)

    job.getConfiguration
  }

  private def prepareScan(filter: Filter) = new Scan().setFilter(filter)

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   *
   * Returns an `RDD[(String, Map[String, Map[String, A]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associates column family and column to
   * the value. Columns which are not found are omitted from the map.
   */
  def hbase[A](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig,
                                               reader: Reads[A]): RDD[(String, Map[String, Map[String, A]])] =
    hbase(table, data, new Scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   * Accepts custom HBase Scan instance
   */
  def hbase[A](table: String, data: Map[String, Set[String]],
               scan: Scan)(implicit config: HBaseConfig, reader: Reads[A]) = {
    val columns = (for {
      (cf, cols) <- data
      col <- cols
    } yield s"$cf:$col") mkString " "

    sc.newAPIHadoopRDD(makeConf(config, table, Some(columns), scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
      case (key, row) =>
        Bytes.toString(key.get) -> extract(data, row, reader)
    }
  }

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   * ccepts HBase filter as a parameter.
   */
  def hbase[A](table: String, data: Map[String, Set[String]],
               filter: Filter)(implicit config: HBaseConfig, reader: Reads[A]): RDD[(String, Map[String, Map[String, A]])] =
    hbase(table, data, prepareScan(filter))

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   *
   * Returns an `RDD[(String, Map[String, Map[String, A]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associated column family and column to
   * the value.
   */
  def hbase[A](table: String, data: Set[String], scan: Scan = new Scan)(implicit config: HBaseConfig,
                                                                        reader: Reads[A]) = {
    val families = data mkString " "

    sc.newAPIHadoopRDD(makeConf(config, table, Some(families), scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
        case (key, row) =>
          Bytes.toString(key.get) -> extractRow(data, row, reader)
      }
  }

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   * Accepts HBase filter as a parameter.
   */
  def hbase[A](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig,
                               reader: Reads[A]): RDD[(String, Map[String, Map[String, A]])] = {
    hbase(table, data, prepareScan(filter))
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
   *
   */
  def hbase(table: String, scan: Scan = new Scan)(implicit config: HBaseConfig) =
    sc.newAPIHadoopRDD(makeConf(config, table, scan = scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
      case (key, row) =>
        Bytes.toString(key.get) -> row
    }

  /**
   * Provides an RDD of HBase rows, without interpreting the content
   * of the rows with HBase filter support
   */
  def hbase(table: String, filter: Filter)(implicit config: HBaseConfig): RDD[(String, Result)] =
    hbase(table, prepareScan(filter))

}