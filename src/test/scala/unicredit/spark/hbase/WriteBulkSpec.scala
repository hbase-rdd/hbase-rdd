package unicredit.spark.hbase

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

import scala.util.Random

class WriteBulkSpec extends FlatSpec with MiniCluster with Checkers with Matchers with BeforeAndAfter {

  before {
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  def nextString = (1 to 10) map (_ => Random.nextPrintableChar) mkString

  val numKeys = 1000
  val numCols = 10
  val numRegions = 4

  val keys = Random.shuffle(0 to numKeys-1) map (i => f"row$i%03d")
  val cols = ((1 to numCols) map (i => f"col$i%02d")).to[Seq] // must be a collection.Seq and not a collection.immutable.Seq
  val family = "cf"
  val families = Seq("cf1", "cf2")

  // one family, fixed columns
  val source = keys map { k => (k, cols map (_ => nextString)) }
  // two families, different columns per family
  val source_multi_cf = source map { case (k, v) =>
    (k, families map { f => f -> cols.map(c => (s"$f$c", nextString)).toMap } toMap)
  }

  val table_prefix = "test_bulk"
  var table_counter = 0

  var splitKeys: Array[String] = _

  "computeSplits" should "create an array of (number of regions - 1) split keys" in {
    val rddKeys = sc.parallelize(keys)
    splitKeys = computeSplits(rddKeys, numRegions).toArray

    splitKeys should have size (numRegions - 1)
  }

  "A HFileRDD" should "write to a Table with 1 HFile per region" in {
    table_counter += 1
    val table_bulk = table_prefix + s"_$table_counter"
    val htable = htu.createTable(table_bulk, family, splitKeys)

    sc.parallelize(source)
      .toHBaseBulk(table_bulk, family, cols)

    checkWithOneColumnFamily(htable, family, cols, source, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with 2 HFiles per region" in {
    table_counter += 1
    val table_bulk = table_prefix + s"_$table_counter"
    val htable = htu.createTable(table_bulk, family, splitKeys)

    sc.parallelize(source)
      .toHBaseBulk(table_bulk, family, cols, 2)

    checkWithOneColumnFamily(htable, family, cols, source, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with timestamps" in {
    table_counter += 1
    val table_bulk = table_prefix + s"_$table_counter"
    val htable = htu.createTable(table_bulk, family, splitKeys)

    val source_ts = source map { case (k, cols) => (k, cols map ((_, 1L))) }

    sc.parallelize(source_ts)
      .toHBaseBulk(table_bulk, family, cols)

    checkWithOneColumnFamily(htable, family, cols, source_ts, (v, ts) => (v, ts))
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with duplicated cells" in {
    table_counter += 1
    val table_bulk = table_prefix + s"_$table_counter"
    val htable = htu.createTable(table_bulk, family, splitKeys)

    val row = source(Random.nextInt(numKeys))
    val source_double_row = row +: source

    sc.parallelize(source_double_row)
      .toHBaseBulk(table_bulk, family, cols)

    checkWithOneColumnFamily(htable, family, cols, source_double_row, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with cells that only differ in timestamp" in {
    table_counter += 1
    val table_bulk = table_prefix + s"_$table_counter"
    val htable = htu.createTable(table_bulk, family, splitKeys)

    val source_ts = source map { case (k, cols) => (k, cols map ((_, 2L))) }
    val row = source_ts(Random.nextInt(numKeys))
    val source_double_row = (row._1, row._2 map { case (s, _) => (s, 1L) }) +: source_ts

    sc.parallelize(source_double_row)
      .toHBaseBulk(table_bulk, family, cols)

    // do not check timestamps, for simplicity
    // (actually, we should check all cells, and not only latest cells)
    checkWithOneColumnFamily(htable, family, cols, source, (v, _) => v)
    htu.deleteTable(table_bulk)
  }

  it should "write to a Table with 2 column families" in {
    table_counter += 1
    val table_bulk = table_prefix + s"_$table_counter"
    val htable = htu.createTable(table_bulk, families.toArray, splitKeys)

    sc.parallelize(source_multi_cf)
      .toHBaseBulk(table_bulk)

    checkWithAllColumnFamilies(htable, source_multi_cf, (v, _) => v)
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
