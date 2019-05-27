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

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import unicredit.spark.hbase.HBaseDeleteMethods._

/**
 * Adds implicit methods to
 * `RDD[K]`,
 * `RDD[(K, Set[Q])]`,
 * `RDD[(K, Set[(Q, Long)])]`,
 * `RDD[(K, Map[String, Set[Q]])]`,
 * `RDD[(K, Map[String, Set[(Q, Long)]])]`,
 * to delete HBase tables.
 */
trait HBaseDeleteSupport {

  implicit def deleteHBaseRDDKey[K: Writes](rdd: RDD[K]): HBaseDeleteRDDKey[K] =
    new HBaseDeleteRDDKey(rdd)

  implicit def deleteHBaseRDDSimple[K: Writes, Q: Writes](rdd: RDD[(K, Set[Q])]): HBaseDeleteRDDSimple[K, Q] =
    new HBaseDeleteRDDSimple(rdd, del[Q])

  implicit def deleteHBaseRDDSimpleT[K: Writes, Q: Writes](rdd: RDD[(K, Set[(Q, Long)])]): HBaseDeleteRDDSimple[K, (Q, Long)] =
    new HBaseDeleteRDDSimple(rdd, delT[Q])

  implicit def deleteHBaseRDD[K: Writes, Q: Writes](rdd: RDD[(K, Map[String, Set[Q]])]): HBaseDeleteRDD[K, Q] =
    new HBaseDeleteRDD(rdd, del[Q])

  implicit def deleteHBaseRDDT[K: Writes, Q: Writes](rdd: RDD[(K, Map[String, Set[(Q, Long)]])]): HBaseDeleteRDD[K, (Q, Long)] =
    new HBaseDeleteRDD(rdd, delT[Q])
}

private[hbase] object HBaseDeleteMethods {
  type Deleter[Q] = (Delete, Array[Byte], Q) => Delete

  // Delete
  def del[Q](delete: Delete, cf: Array[Byte], q: Q)(implicit wq: Writes[Q]): Delete = delete.addColumns(cf, wq.write(q))
  def delT[Q](delete: Delete, cf: Array[Byte], qt: (Q, Long))(implicit wq: Writes[Q]): Delete = delete.addColumn(cf, wq.write(qt._1), qt._2)
}

sealed abstract class HBaseDeleteHelpers {
  protected def convert[K, Q](id: K, values: Map[String, Set[Q]], del: Deleter[Q])
                             (implicit wk: Writes[K], ws: Writes[String]): Option[(ImmutableBytesWritable, Delete)] = {
    val d = new Delete(wk.write(id))
    var empty = true
    for {
      (family, contents) <- values
      fb = ws.write(family)
      content <- contents
    } {
      empty = false
      del(d, fb, content)
    }

    if (empty) None else Some(new ImmutableBytesWritable, d)
  }

  protected def convert[K](id: K, families: Set[String])
                          (implicit wk: Writes[K], ws: Writes[String]): Option[(ImmutableBytesWritable, Delete)] = {
    val d = new Delete(wk.write(id))
    for (family <- families) d.addFamily(ws.write(family))
    Some(new ImmutableBytesWritable, d)
  }

  protected def convert[K](id: K)(implicit wk: Writes[K]): Option[(ImmutableBytesWritable, Delete)] = {
    val d = new Delete(wk.write(id))
    Some(new ImmutableBytesWritable, d)
  }
}

final class HBaseDeleteRDDKey[K: Writes](val rdd: RDD[K]) extends HBaseDeleteHelpers with Serializable {

  /**
   * Delete rows specified by rowkeys of the underlying RDD from HBase.
   *
   * The RDD is assumed to be an instance of `RDD[K]` of rowkeys.
   */
  def deleteHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
   * Delete column families of the underlying RDD from HBase.
   *
   * The RDD is assumed to be an instance of `RDD[K]` of rowkeys.
   */
  def deleteHBase(table: String, families: Set[String])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, families) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
   * Delete columns of the underlying RDD from HBase.
   *
   * Columns are deleted from the same column family, and are the same for all rows.
   *
   * The RDD is assumed to be an instance of `RDD[K]` of rowkeys.
   */
  def deleteHBase[Q: Writes](table: String, family: String, columns: Set[Q])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, Map(family -> columns), del[Q]) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
   * Delete columns of the underlying RDD from HBase.
   *
   * Columns specified as a map of families / set of qualifiers, are the same for all rows.
   *
   * The RDD is assumed to be an instance of `RDD[K]` of rowkeys.
   */
  def deleteHBase[Q: Writes](table: String, qualifiers: Map[String, Set[Q]])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, qualifiers, del[Q]) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseDeleteRDDSimple[K: Writes, Q](val rdd: RDD[(K, Set[Q])], val del: Deleter[Q]) extends HBaseDeleteHelpers with Serializable {

  /**
   * Delete columns of the underlying RDD from HBase.
   *
   * Simplified form, where columns are deleted from the
   * same column family.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Set[Q])]`,
   * where the first value is the rowkey and the second is a set of
   * column names w/ or w/o timestamps. If timestamp is specified, only the
   * corresponding version is deleted, otherwise all versions are deleted.
   */
  def deleteHBase(table: String, family: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseDeleteRDD[K: Writes, Q](val rdd: RDD[(K, Map[String, Set[Q]])], val del: Deleter[Q]) extends HBaseDeleteHelpers with Serializable {
  /**
   * Delete columns of the underlying RDD from HBase.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Map[String, Set[Q]])]`,
   * where the first value is the rowkey and the second is a map that associates
   * column families and a set of column names w/ or w/o timestamps.
   * If timestamp is specified, only the corresponding version is deleted,
   * otherwise all versions are deleted.
   */
  def deleteHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}