package unicredit.spark.hbase

import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.spark.SparkContext
import org.scalatest.{FlatSpec, BeforeAndAfter, Matchers}

class ReadWriteSpec extends FlatSpec with MiniCluster with Checkers with Matchers with BeforeAndAfter {

  before {
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  // some column families
  val families = Set("cf1", "cf2")
  // a column family
  val a_cf = families.head
  // some columns
  val cols = Seq("col1", "col2", "col3")
  // a timestamp
  val a_ts = 1L
  // a data set with all column families
  val source = Seq(
    ("row1", families map { f => f -> Map(cols(0) -> "val11", cols(1) -> "val12") } toMap),
    ("row2", families map { f => f -> Map(cols(0) -> "val21", cols(2) -> "val23") } toMap),
    ("row3", families map { f => f -> Map(cols(1) -> "val32", cols(2) -> "val33") } toMap))
  // a data set with one column family
  val source_one_cf = source map { case (k, m) => (k, m(a_cf)) }
  // a data set with all column families with timestamps
  val source_ts = source map { case (k, cf) => (k, cf map { case (cf, d) => (cf, d map { case (c, v) => (c, (v, a_ts)) }) }) }
  // a data set with one column family with timestamps
  val source_one_cf_ts = source_ts map { case (k, m) => (k, m(a_cf)) }
  // a row key
  val a_key = source(1)._1

  // a table for testing all column families
  val table_all_cf = "test_all_cf"
  // a table for testing one column family
  val table_one_cf = "test_one_cf"
  // a table for testing all column families with timestamps
  val table_all_cf_ts = "test_all_cf_ts"
  // a table for testing one column family with timestamps
  val table_one_cf_ts = "test_one_cf_ts"

  // a filter for reading one row
  val filter = new PrefixFilter(a_key)

  "A HBaseRDD" should "write to a Table all column families" in {
    val htable = htu.createTable(table_all_cf, families.toArray)
    sc.parallelize(source)
      .toHBase(table_all_cf)
    checkWithAllColumnFamilies(htable, source, (v, _) => v)
  }

  it should "write to a Table one column family" in {
    val htable = htu.createTable(table_one_cf, a_cf)
    sc.parallelize(source_one_cf)
      .toHBase(table_one_cf, a_cf)
    checkWithOneColumnFamily(htable, a_cf, source_one_cf, (v, _) => v)
  }

  it should "read from a Table all columns in column families" in {
    val res = sc.hbase[String](table_all_cf, families)
      .collect()
    res should have size source.size
    res should === (source)
  }

  it should "read from a Table a set of columns in column families" in {
    val res = sc.hbase[String](table_all_cf, families map (_ -> cols.toSet) toMap)
      .collect()
    res should have size source.size
    res should === (source)
  }

  it should "read with filter from a Table all columns in column families" in {
    val res = sc.hbase[String](table_all_cf, families, filter)
      .collect()
    res should have size 1
    res should === (source filter (_._1 == a_key))
  }

  it should "read with filter from a Table a set of columns in column families" in {
    val res = sc.hbase[String](table_all_cf, families map (_ -> cols.toSet) toMap, filter)
      .collect()
    res should have size 1
    res should === (source filter (_._1 == a_key))
  }

  // test again with timestamps
  "A HBaseRDD with timestamps" should "write to a Table all column families" in {
    val htable = htu.createTable(table_all_cf_ts, families.toArray)
    sc.parallelize(source_ts)
      .toHBase(table_all_cf_ts)
    checkWithAllColumnFamilies(htable, source_ts, (v, ts) => (v, ts))
  }

  it should "write to a Table one column family" in {
    val htable = htu.createTable(table_one_cf_ts, a_cf)
    sc.parallelize(source_one_cf_ts)
      .toHBase(table_one_cf_ts, a_cf)
    checkWithOneColumnFamily(htable, a_cf, source_one_cf_ts, (v, ts) => (v, ts))
  }

  it should "read from a Table all columns in column families" in {
    val res = sc.hbaseTS[String](table_all_cf_ts, families)
      .collect()
    res should have size source_ts.size
    res should === (source_ts)
  }

  it should "read from a Table a set of columns in column families" in {
    val res = sc.hbaseTS[String](table_all_cf_ts, families map (_ -> cols.toSet) toMap)
      .collect()
    res should have size source_ts.size
    res should === (source_ts)
  }

  it should "read with filter from a Table all columns in column families" in {
    val res = sc.hbaseTS[String](table_all_cf_ts, families, filter)
      .collect()
    res should have size 1
    res should === (source_ts filter (_._1 == a_key))
  }

  it should "read with filter from a Table a set of columns in column families" in {
    val res = sc.hbaseTS[String](table_all_cf_ts, families map (_ -> cols.toSet) toMap, filter)
      .collect()
    res should have size 1
    res should === (source_ts filter (_._1 == a_key))
  }
}
