package unicredit.spark.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Get, Table}

import org.scalatest.{Matchers, Suite, SuiteMixin}

import scala.collection.JavaConverters._

trait Checkers extends SuiteMixin with Matchers with DefaultReads with DefaultWrites { this: Suite =>

  def checkValue(v: String, ts: Long): Any = v
  def checkValueAndTimestamp(v: String, ts: Long): Any = (v, ts)

  // one family
  // map of qualifiers -> values
  def checkWithOneColumnFamily[K, Q, V](t: Table, cf: String, s: Seq[(K, Map[Q, _])], dataToCheck: (V, Long) => Any)
                                       (implicit rk: Reads[K], wk: Writes[K], wq: Writes[Q], rv: Reads[V], ws: Writes[String]): Unit = {
    val cfb = ws.write(cf)

    for ((r, m) <- s) {
      val get = new Get(wk.write(r))
      val result = t.get(get)

      rk.read(result.getRow) should === (r)

      for {
        col <- m.keys
        cell = result.getColumnLatestCell(cfb, wq.write(col))
        value = rv.read(CellUtil.cloneValue(cell))
        ts = cell.getTimestamp
      } dataToCheck(value, ts) should === (m(col))
    }
  }

  // one family
  // fixed columns
  def checkWithOneColumnFamily[K, Q, V](t: Table, cf: String, cols: Seq[Q], s: Seq[(K, Seq[_])], dataToCheck: (V, Long) => Any)
                                       (implicit rk: Reads[K], wk: Writes[K], wq: Writes[Q], rv: Reads[V], ws: Writes[String]): Unit = {
    val cfb = ws.write(cf)

    for ((r, vs) <- s) {
      val get = new Get(wk.write(r))
      val result = t.get(get)

      rk.read(result.getRow) should === (r)

      val data = cols zip vs

      for {
        (col, v) <- data
        cell = result.getColumnLatestCell(cfb, wq.write(col))
        value = rv.read(CellUtil.cloneValue(cell))
        ts = cell.getTimestamp
      } dataToCheck(value, ts) should === (v)
    }
  }

  // many families
  // map of qualifiers -> values
  def checkWithAllColumnFamilies[K, Q, V](t: Table, s: Seq[(K, Map[String, Map[Q, _]])], dataToCheck: (V, Long) => Any)
                                         (implicit rk: Reads[K], wk: Writes[K], wq: Writes[Q], rv: Reads[V], ws: Writes[String]): Unit = {
    for ((r, m) <- s) {
      val get = new Get(wk.write(r))
      val result = t.get(get)

      rk.read(result.getRow) should === (r)

      for {
        cf <- m.keys
        cfb = ws.write(cf)
        col <- m(cf).keys
        cell = result.getColumnLatestCell(cfb, wq.write(col))
        value = rv.read(CellUtil.cloneValue(cell))
        ts = cell.getTimestamp
      } dataToCheck(value, ts) should === (m(cf)(col))
    }
  }

  // one family
  // fixed columns, values with timestamp
  def checkWithOneColumnFamilyAndTimestamp[K, Q, V](t: Table, cf: String, cols: Seq[Q], s: Seq[(K, Seq[(V, Long)])])
                                                   (implicit rk: Reads[K], wk: Writes[K], wq: Writes[Q], rv: Reads[V], ws: Writes[String]): Unit = {
    val cfb = ws.write(cf)

    for ((r, vs) <- s) {
      val get = new Get(wk.write(r))
      get.setMaxVersions(2)
      val result = t.get(get)

      rk.read(result.getRow) should === (r)

      val data = cols zip vs

      for {
        (col, (value, timestamp)) <- data
        cells = result.getColumnCells(cfb, wq.write(col)).asScala
      } cells.map { cell =>
        val cellValue = rv.read(CellUtil.cloneValue(cell))
        val cellTimestamp = cell.getTimestamp
        (cellValue, cellTimestamp)
      } should contain ((value, timestamp))
    }
  }
}
