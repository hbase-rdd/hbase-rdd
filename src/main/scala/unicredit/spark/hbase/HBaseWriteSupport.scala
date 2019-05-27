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

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import HBaseWriteMethods._

/**
 * Adds implicit methods to
 * `RDD[(K, Map[Q, V])]`,
 * `RDD[(K, Seq[V])]`,
 * `RDD[(K, Map[String, Map[Q, V]])]`
 * `RDD[(K, Map[Q, (V, Long)])]`,
 * `RDD[(K, Seq[(V, Long)])]`,
 * `RDD[(K, Map[String, Map[Q, (V, Long)]])]`
 * to write to HBase tables.
 */
trait HBaseWriteSupport {

  implicit def toHBaseRDDSimple[K: Writes, Q: Writes, V: Writes](rdd: RDD[(K, Map[Q, V])]): HBaseWriteRDDSimple[K, Q, V] =
    new HBaseWriteRDDSimple(rdd, pa[V])

  implicit def toHBaseRDDSimpleTS[K: Writes, Q: Writes, V: Writes](rdd: RDD[(K, Map[Q, (V, Long)])]): HBaseWriteRDDSimple[K, Q, (V, Long)] =
    new HBaseWriteRDDSimple(rdd, pa[V])

  implicit def toHBaseRDDFixed[K: Writes, V: Writes](rdd: RDD[(K, Seq[V])]): HBaseWriteRDDFixed[K, V] =
    new HBaseWriteRDDFixed(rdd, pa[V])

  implicit def toHBaseRDDFixedTS[K: Writes, V: Writes](rdd: RDD[(K, Seq[(V, Long)])]): HBaseWriteRDDFixed[K, (V, Long)] =
    new HBaseWriteRDDFixed(rdd, pa[V])

  implicit def toHBaseRDD[K: Writes, Q: Writes, V: Writes](rdd: RDD[(K, Map[String, Map[Q, V]])]): HBaseWriteRDD[K, Q, V] =
    new HBaseWriteRDD(rdd, pa[V])

  implicit def toHBaseRDDT[K: Writes, Q: Writes, V: Writes](rdd: RDD[(K, Map[String, Map[Q, (V, Long)]])]): HBaseWriteRDD[K, Q, (V, Long)] =
    new HBaseWriteRDD(rdd, pa[V])
}

private[hbase] object HBaseWriteMethods {
  type PutAdder[V] = (Put, Array[Byte], Array[Byte], V) => Put

  // PutAdder
  def pa[V](put: Put, cf: Array[Byte], q: Array[Byte], v: V)(implicit wv: Writes[V]): Put = put.addColumn(cf, q, wv.write(v))
  def pa[V](put: Put, cf: Array[Byte], q: Array[Byte], v: (V, Long))(implicit wv: Writes[V]): Put = put.addColumn(cf, q, v._2, wv.write(v._1))
}

sealed abstract class HBaseWriteHelpers {
  protected def convert[K, Q, V](id: K, values: Map[String, Map[Q, V]], put: PutAdder[V])
                                (implicit wk: Writes[K], wq: Writes[Q], ws: Writes[String]): Option[(ImmutableBytesWritable, Put)] = {
    val p = new Put(wk.write(id))
    var empty = true
    for {
      (family, content) <- values
      fb = ws.write(family)
      (qualifier, value) <- content
    } {
      empty = false
      put(p, fb, wq.write(qualifier), value)
    }

    if (empty) None else Some(new ImmutableBytesWritable, p)
  }
}

final class HBaseWriteRDDSimple[K: Writes, Q: Writes, V](val rdd: RDD[(K, Map[Q, V])], val put: PutAdder[V]) extends HBaseWriteHelpers with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * Simplified form, where all values are written to the
   * same column family.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Map[Q, V])]`,
   * where the first value is the rowkey and the second is a map that
   * associates column names to values.
   */
  def toHBase(table: String, family: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseWriteRDDFixed[K: Writes, V](val rdd: RDD[(K, Seq[V])], val put: PutAdder[V]) extends HBaseWriteHelpers with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * Simplified form, where all values are written to the
   * same column family, and columns are fixed, so that their names can be passed as a sequence.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Seq[V])]`,
   * where the first value is the rowkey and the second is a sequence of values
   * that are associated to a sequence of headers.
   */
  def toHBase[Q: Writes](table: String, family: String, headers: Seq[Q])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    val sc = rdd.context
    val bheaders = sc.broadcast(headers)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> Map(bheaders.value zip v: _*)), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseWriteRDD[K: Writes, Q: Writes, V](val rdd: RDD[(K, Map[String, Map[Q, V]])], val put: PutAdder[V]) extends HBaseWriteHelpers with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * The RDD is assumed to be an instance of `RDD[(K, Map[String, Map[Q, V]])]`,
   * where the first value is the rowkey and the second is a nested map that associates
   * column families and column names to values.
   */
  def toHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}