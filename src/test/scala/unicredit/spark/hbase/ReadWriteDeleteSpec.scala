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

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ReadWriteDeleteSpec extends FlatSpec with MiniCluster with Checkers with Matchers with BeforeAndAfter {

  before {
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  val w = implicitly[Writes[Int]]

  // some column families
  val families = Set("cf1", "cf2")
  // a column family
  val a_cf = families.head
  // some columns
  val cols = Array(1, 2, 3)
  // a timestamp
  val a_ts = 1L
  // a data set with all column families
  val source = Seq(
    (1, families map { f => f -> Map(cols(0) -> "val11", cols(1) -> "val12") } toMap),
    (2, families map { f => f -> Map(cols(0) -> "val21", cols(2) -> "val23") } toMap),
    (3, families map { f => f -> Map(cols(1) -> "val32", cols(2) -> "val33") } toMap))
  // a data set with one column family
  val source_one_cf = source map { case (k, m) => (k, m(a_cf)) }
  // a data set with all column families with timestamps
  val source_ts = source map { case (k, cfm) => (k, cfm map { case (cf, d) => (cf, d map { case (c, v) => (c, (v, a_ts)) }) }) }
  // a data set with one column family with timestamps
  val source_one_cf_ts = source_ts map { case (k, m) => (k, m(a_cf)) }
  // a row key
  val a_key = source(1)._1

  // a table for testing all column families
  val table_all_cf = TableName.valueOf("test_all_cf")
  // a table for testing one column family
  val table_one_cf = TableName.valueOf("test_one_cf")
  // a table for testing all column families with timestamps
  val table_all_cf_ts = TableName.valueOf("test_all_cf_ts")
  // a table for testing one column family with timestamps
  val table_one_cf_ts = TableName.valueOf("test_one_cf_ts")

  // a filter for reading one row
  val filter = new PrefixFilter(w.write(a_key))

  "A HBaseRDD" should "write to a Table all column families" in {
    val htable = htu.createTable(table_all_cf, families.toArray)
    sc.parallelize(source)
      .toHBase(table_all_cf.toString)
    checkWithAllColumnFamilies(htable, source, checkValue)
  }

  it should "write to a Table one column family" in {
    val table = htu.createTable(table_one_cf, a_cf)
    sc.parallelize(source_one_cf)
      .toHBase(table_one_cf.toString, a_cf)
    checkWithOneColumnFamily(table, a_cf, source_one_cf, checkValue)
  }

  it should "read from a Table all columns in column families" in {
    val res = sc.hbase[Int, Int, String](table_all_cf.toString, families)
      .collect()
    res should have size source.size
    res should === (source)
  }

  it should "read from a Table a set of columns in column families" in {
    val res = sc.hbase[Int, Int, String](table_all_cf.toString, families map (_ -> cols.toSet) toMap)
      .collect()
    res should have size source.size
    res should === (source)
  }

  it should "read with filter from a Table all columns in column families" in {
    val res = sc.hbase[Int, Int, String](table_all_cf.toString, families, filter)
      .collect()
    res should have size 1
    res should === (source filter (_._1 == a_key))
  }

  it should "read with filter from a Table a set of columns in column families" in {
    val res = sc.hbase[Int, Int, String](table_all_cf.toString, families map (_ -> cols.toSet) toMap, filter)
      .collect()
    res should have size 1
    res should === (source filter (_._1 == a_key))
  }

  it should "delete all columns of all column families from a Table" in {
    sc.parallelize(source map { case (k, _) => (k, families.map(cf => (cf, cols.toSet)).toMap) })
      .deleteHBase(table_all_cf.toString)
    val res = sc.hbase[Int, Int, String](table_all_cf.toString, families)
      .collect()
    res should have size 0
  }

  it should "delete all columns of one column family from a Table" in {
    sc.parallelize(source_one_cf map { case (k, c) => (k, c.keySet) })
      .deleteHBase(table_one_cf.toString, a_cf)
    val res = sc.hbase[Int, Int, String](table_one_cf.toString, Set(a_cf))
      .collect()
    res should have size 0
  }

  // test again with timestamps
  "A HBaseRDD with timestamps" should "write to a Table all column families" in {
    val table = htu.createTable(table_all_cf_ts, families.toArray)
    sc.parallelize(source_ts)
      .toHBase(table_all_cf_ts.toString)
    checkWithAllColumnFamilies(table, source_ts, checkValueAndTimestamp)
  }

  it should "write to a Table one column family" in {
    val htable = htu.createTable(table_one_cf_ts, a_cf)
    sc.parallelize(source_one_cf_ts)
      .toHBase(table_one_cf_ts.toString, a_cf)
    checkWithOneColumnFamily(htable, a_cf, source_one_cf_ts, checkValueAndTimestamp)
  }

  it should "read from a Table all columns in column families" in {
    val res = sc.hbaseTS[Int, Int, String](table_all_cf_ts.toString, families)
      .collect()
    res should have size source_ts.size
    res should === (source_ts)
  }

  it should "read from a Table a set of columns in column families" in {
    val res = sc.hbaseTS[Int, Int, String](table_all_cf_ts.toString, families map (_ -> cols.toSet) toMap)
      .collect()
    res should have size source_ts.size
    res should === (source_ts)
  }

  it should "read with filter from a Table all columns in column families" in {
    val res = sc.hbaseTS[Int, Int, String](table_all_cf_ts.toString, families, filter)
      .collect()
    res should have size 1
    res should === (source_ts filter (_._1 == a_key))
  }

  it should "read with filter from a Table a set of columns in column families" in {
    val res = sc.hbaseTS[Int, Int, String](table_all_cf_ts.toString, families map (_ -> cols.toSet) toMap, filter)
      .collect()
    res should have size 1
    res should === (source_ts filter (_._1 == a_key))
  }

  it should "delete all columns of all column families from a Table" in {
    sc.parallelize(source_ts map { case (k, m) => (k, m map { case (cf, cvt) => (cf, cvt map { case (c, (v, t)) => (c, t) } toSet) }) })
      .deleteHBase(table_all_cf_ts.toString)
    val res = sc.hbaseTS[Int, Int, String](table_all_cf_ts.toString, families)
      .collect()
    res should have size 0
  }

  it should "delete all columns of one column family from a Table" in {
    sc.parallelize(source_one_cf_ts map { case (k, cvt) => (k, cvt map { case (c, (v, t)) => (c, t) } toSet) })
      .deleteHBase(table_one_cf_ts.toString, a_cf)
    val res = sc.hbaseTS[Int, Int, String](table_one_cf_ts.toString, Set(a_cf))
      .collect()
    res should have size 0
  }
}
