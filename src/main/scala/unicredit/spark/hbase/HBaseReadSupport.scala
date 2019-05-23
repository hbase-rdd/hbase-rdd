/* Copyright 2019 UniCredit S.p.A.
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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD

import org.apache.hadoop.hbase.{ Cell, CellUtil }
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.mapreduce.{ TableInputFormat, IdentityTableMapper, TableMapReduceUtil }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._

/**
 * Adds implicit methods to SparkContext to read
 * from HBase sources.
 */
trait HBaseReadSupport {
  implicit def toHBaseSC(sc: SparkContext): HBaseSC = new HBaseSC(sc)
}

final class HBaseSC(@transient sc: SparkContext) extends Serializable {
  private def extract[Q, V](data: Map[String, Set[Q]], result: Result, read: Cell => V)(implicit wq: Writes[Q], ws: Writes[String]) = {
    data map {
      case (cf, columns) =>
        val content = columns flatMap { column =>
          Option {
            result.getColumnLatestCell(ws.write(cf), wq.write(column))
          } map { cell =>
            column -> read(cell)
          }
        } toMap

        cf -> content
    }
  }

  private def extractRow[Q, V](data: Set[String], result: Result, read: Cell => V)(implicit rq: Reads[Q], rs: Reads[String]) = {
    result.listCells.asScala groupBy { cell =>
      rs.read(CellUtil.cloneFamily(cell))
    } filterKeys data.contains map {
      // We cannot use mapValues here, because it returns a MapLike, which is not serializable,
      // instead we need a (serializable) Map (see https://issues.scala-lang.org/browse/SI-7005)
      case (k, cells) =>
        (k, cells map { cell =>
          val column = rq.read(CellUtil.cloneQualifier(cell))
          column -> read(cell)
        } toMap)
    }
  }

  private def read[V](cell: Cell)(implicit rv: Reads[V]) = {
    val value = CellUtil.cloneValue(cell)
    rv.read(value)
  }

  private def readTS[V](cell: Cell)(implicit rv: Reads[V]) = {
    val value = CellUtil.cloneValue(cell)
    val timestamp = cell.getTimestamp
    (rv.read(value), timestamp)
  }

  private def makeConf(config: HBaseConfig, table: String, columns: Option[String] = None, scan: Scan = new Scan) = {
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
   * Returns an `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]`
   * or `RDD[(String, Map[String, Map[String, V]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associates column family and column to
   * the value. Columns which are not found are omitted from the map.
   */
  def hbase[K: Reads, Q: Writes, V: Reads](table: String, data: Map[String, Set[Q]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, new Scan)
  def hbase[K: Reads, V: Reads](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, new Scan)
  def hbase[V: Reads](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, new Scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   * Accepts HBase filter as a parameter.
   */
  def hbase[K: Reads, Q: Writes, V: Reads](table: String, data: Map[String, Set[Q]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, prepareScan(filter))
  def hbase[K: Reads, V: Reads](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, prepareScan(filter))
  def hbase[V: Reads](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, prepareScan(filter))

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   * Accepts custom HBase Scan instance
   */
  def hbase[K: Reads, Q: Writes, V: Reads](table: String, data: Map[String, Set[Q]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] = {
    val rk = implicitly[Reads[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extract(data, row, read[V])
    }
  }
  def hbase[K: Reads, V: Reads](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, scan)
  def hbase[V: Reads](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   *
   * Returns an `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]`
   * or `RDD[(String, Map[String, Map[String, (V, Long)]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associates column family and column to
   * the tuple (value, timestamp). Columns which are not found are omitted from the map.
   */
  def hbaseTS[K: Reads, Q: Writes, V: Reads](table: String, data: Map[String, Set[Q]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, new Scan)
  def hbaseTS[K: Reads, V: Reads](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, new Scan)
  def hbaseTS[V: Reads](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, new Scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   * Accepts HBase filter as a parameter.
   */
  def hbaseTS[K: Reads, Q: Writes, V: Reads](table: String, data: Map[String, Set[Q]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, prepareScan(filter))
  def hbaseTS[K: Reads, V: Reads](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, prepareScan(filter))
  def hbaseTS[V: Reads](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, prepareScan(filter))

  /**
   * Provides an RDD of HBase rows. Here `data` is a map whose
   * keys represent the column families and whose values are
   * the list of columns to be read from the family.
   * Accepts custom HBase Scan instance
   */
  def hbaseTS[K: Reads, Q: Writes, V: Reads](table: String, data: Map[String, Set[Q]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] = {
    val rk = implicitly[Reads[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extract(data, row, readTS[V])
    }
  }
  def hbaseTS[K: Reads, V: Reads](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, scan)
  def hbaseTS[V: Reads](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, scan)

  protected def hbaseRaw[Q: Writes](table: String, data: Map[String, Set[Q]], scan: Scan)(implicit config: HBaseConfig): RDD[(ImmutableBytesWritable, Result)] = {
    val columns = (for {
      (cf, cols) <- data
      col <- cols
    } yield s"$cf:$col") mkString " "

    sc.newAPIHadoopRDD(makeConf(config, table, Some(columns), scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
  }

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   *
   * Returns an `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]`
   * or `RDD[(String, Map[String, Map[String, V]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associated column family and column to
   * the value.
   */
  def hbase[K: Reads, Q: Reads, V: Reads](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, new Scan)
  def hbase[K: Reads, V: Reads](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, new Scan)
  def hbase[V: Reads](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, new Scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   * Accepts HBase filter as a parameter.
   */
  def hbase[K: Reads, Q: Reads, V: Reads](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, prepareScan(filter))
  def hbase[K: Reads, V: Reads](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, prepareScan(filter))
  def hbase[V: Reads](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, prepareScan(filter))

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   * Accepts custom HBase Scan instance
   */
  def hbase[K: Reads, Q: Reads, V: Reads](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] = {
    val rk = implicitly[Reads[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extractRow(data, row, read[V])
    }
  }
  def hbase[K: Reads, V: Reads](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, scan)
  def hbase[V: Reads](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   *
   * Returns an `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]`
   * or `RDD[(String, Map[String, Map[String, (V, Long)]])]`, where
   * the first element is the rowkey and the second element is a
   * nested map which associated column family and column to
   * the tuple (value, timestamp).
   */
  def hbaseTS[K: Reads, Q: Reads, V: Reads](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, new Scan)
  def hbaseTS[K: Reads, V: Reads](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, new Scan)
  def hbaseTS[V: Reads](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, new Scan)

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   * Accepts HBase filter as a parameter.
   */
  def hbaseTS[K: Reads, Q: Reads, V: Reads](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, prepareScan(filter))
  def hbaseTS[K: Reads, V: Reads](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, prepareScan(filter))
  def hbaseTS[V: Reads](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, prepareScan(filter))

  /**
   * Provides an RDD of HBase rows. Here `data` is a set of
   * column families, which are read in full.
   * Accepts custom HBase Scan instance
   */
  def hbaseTS[K: Reads, Q: Reads, V: Reads](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] = {
    val rk = implicitly[Reads[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extractRow(data, row, readTS[V])
    }
  }
  def hbaseTS[K: Reads, V: Reads](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, scan)
  def hbaseTS[V: Reads](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, scan)

  protected def hbaseRaw(table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(ImmutableBytesWritable, Result)] = {
    val families = data mkString " "

    sc.newAPIHadoopRDD(makeConf(config, table, Some(families), scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
  }

  /**
   * Provides an RDD of HBase rows, without interpreting the content
   * of the rows.
   *
   * Returns an `RDD[(K, Result)]`, where the first element is the
   * rowkey and the second element is an instance of
   * `org.apache.hadoop.hbase.client.Result`.
   *
   * The client can then use the full HBase API to process the result.
   */
  def hbase[K: Reads](table: String, scan: Scan = new Scan)(implicit config: HBaseConfig): RDD[(K, Result)] = {
    val rk = implicitly[Reads[K]]
    sc.newAPIHadoopRDD(makeConf(config, table, scan = scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
      case (key, row) =>
        rk.read(key.get) -> row
    }
  }

  /**
   * Provides an RDD of HBase rows, without interpreting the content
   * of the rows, with HBase filter support
   */
  def hbase[K: Reads](table: String, filter: Filter)(implicit config: HBaseConfig): RDD[(K, Result)] = hbase(table, prepareScan(filter))

}