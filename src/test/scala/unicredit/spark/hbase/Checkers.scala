package unicredit.spark.hbase

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{SuiteMixin, Matchers, Suite}

trait Checkers extends SuiteMixin with Matchers { this: Suite =>
  // one family
  // map of qualifiers -> values
  def checkWithOneColumnFamily(t: HTable, cf: String, s: Seq[(String, Map[String, _])], dataToCheck: (String, Long) => Any) = {
    for ((r, m) <- s) {
      val get = new Get(r)
      val result = t.get(get)

      Bytes.toString(result.getRow()) should === (r)

      for {
        col <- m.keys
        cell = result.getColumnLatestCell(cf, col)
        value = Bytes.toString(CellUtil.cloneValue(cell))
        ts = cell.getTimestamp
      } dataToCheck(value, ts) should === (m(col))
    }
  }

  // one family
  // fixed columns
  def checkWithOneColumnFamily(t: HTable, cf: String, cols: Seq[String], s: Seq[(String, Seq[_])], dataToCheck: (String, Long) => Any) = {
    for ((r, vs) <- s) {
      val get = new Get(r)
      val result = t.get(get)

      Bytes.toString(result.getRow()) should === (r)

      val data = cols zip vs

      for {
        (col, v) <- data
        cell = result.getColumnLatestCell(cf, col)
        value = Bytes.toString(CellUtil.cloneValue(cell))
        ts = cell.getTimestamp
      } dataToCheck(value, ts) should === (v)
    }
  }

  // many families
  // map of qualifiers -> values
  def checkWithAllColumnFamilies(t: HTable, s: Seq[(String, Map[String, Map[String, _]])], dataToCheck: (String, Long) => Any) = {
    for ((r, m) <- s) {
      val get = new Get(r)
      val result = t.get(get)

      Bytes.toString(result.getRow()) should === (r)

      for {
        cf <- m.keys
        col <- m(cf).keys
        cell = result.getColumnLatestCell(cf, col)
        value = Bytes.toString(CellUtil.cloneValue(cell))
        ts = cell.getTimestamp
      } dataToCheck(value, ts) should === (m(cf)(col))
    }
  }
}
