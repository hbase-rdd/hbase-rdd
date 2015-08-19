package unicredit.spark.hbase

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.util.Random

class WriteBulkSpec extends FlatSpec with MiniCluster with Matchers with BeforeAndAfter {

  before {
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

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

  def nextString = (1 to 10) map (_ => Random.nextPrintableChar) mkString

  val numKeys = 1000
  val numCols = 10
  val numRegions = 4

  val keys = Random.shuffle(0 to numKeys-1) map (i => f"row$i%03d")
  val cols = ((1 to numCols) map (i => f"col$i%02d")).to[Seq] // must be a collection.Seq and not a collection.immutable.Seq
  val cf = "cf"

  val source = keys map { k => (k, cols map (_ => nextString)) }

  val table_prefix = "test_bulk"

  var splitKeys: Array[String] = _

  "computeSplits" should "create an array of (number of regions - 1) split keys" in {
    val rddKeys = sc.parallelize(keys)
    splitKeys = computeSplits(rddKeys, numRegions).toArray

    splitKeys should have size (numRegions - 1)
  }

  "A HFileRDD" should "write to a Table with 1 HFile per region" in {
    val table_bulk = table_prefix + "_1"
    val htable = htu.createTable(table_bulk, cf, splitKeys)

    sc.parallelize(source)
      .toHBaseBulk(table_bulk, cf, cols)

    checkWithOneColumnFamily(htable, cf, cols, source, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with 2 HFiles per region" in {
    val table_bulk = table_prefix + "_2"
    val htable = htu.createTable(table_bulk, cf, splitKeys)

    sc.parallelize(source)
      .toHBaseBulk(table_bulk, cf, cols, 2)

    checkWithOneColumnFamily(htable, cf, cols, source, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with timestamps" in {
    val table_bulk = table_prefix + "_3"
    val htable = htu.createTable(table_bulk, cf, splitKeys)

    val source_ts = source map { case (k, cols) => (k, cols map ((_, 1L))) }

    sc.parallelize(source_ts)
      .toHBaseBulk(table_bulk, cf, cols)

    checkWithOneColumnFamily(htable, cf, cols, source_ts, (v, ts) => (v, ts))
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with duplicated cells" in {
    val table_bulk = table_prefix + "_4"
    val htable = htu.createTable(table_bulk, cf, splitKeys)

    val row = source(Random.nextInt(numKeys))
    val source_double_row = row +: source

    sc.parallelize(source_double_row)
      .toHBaseBulk(table_bulk, cf, cols)

    checkWithOneColumnFamily(htable, cf, cols, source_double_row, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with cells that only differ in timestamp" in {
    val table_bulk = table_prefix + "_5"
    val htable = htu.createTable(table_bulk, cf, splitKeys)

    val source_ts = source map { case (k, cols) => (k, cols map ((_, 2L))) }
    val row = source_ts(Random.nextInt(numKeys))
    val source_double_row = (row._1, row._2 map { case (s, _) => (s, 1L) }) +: source_ts

    sc.parallelize(source_double_row)
      .toHBaseBulk(table_bulk, cf, cols)

    // do not check timestamps, for simplicity
    // (actually, we should check all cells, and not only latest cells)
    checkWithOneColumnFamily(htable, cf, cols, source, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "delete all temporary files in the end" in {
    FileSystem.get(conf.get).close() // close the file system, so that deleteOnExit is triggered
    val fs = FileSystem.get(conf.get)
    val path = new Path("/tmp")
    val listFiles = fs.listStatus(path)
    listFiles foreach { f =>
      val name = f.getPath.getName
      name should not startWith table_prefix
      name should not startWith "partitions_" // temporary files created by HFileOutputFormat2
    }
  }
}
