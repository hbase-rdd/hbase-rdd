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

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD

import HBaseWriteMethods._

/**
 * Adds implicit methods to `RDD[(String, Map[String, A])]`,
 * `RDD[(String, Seq[A])]` and
 * `RDD[(String, Map[String, Map[String, A]])]`
 * to write to HBase sources.
 */
trait HBaseWriteSupport {

  implicit def toHBaseRDDSimple[A](rdd: RDD[(String, Map[String, A])])(implicit writer: Writes[A]): HBaseWriteRDDSimple[A] =
    new HBaseWriteRDDSimple(rdd, pa[A])

  implicit def toHBaseRDDSimpleTS[A](rdd: RDD[(String, Map[String, (A, Long)])])(implicit writer: Writes[A]): HBaseWriteRDDSimple[(A, Long)] =
    new HBaseWriteRDDSimple(rdd, pa[A])

  implicit def toHBaseRDDFixed[A](rdd: RDD[(String, Seq[A])])(implicit writer: Writes[A]): HBaseWriteRDDFixed[A] =
    new HBaseWriteRDDFixed(rdd, pa[A])

  implicit def toHBaseRDDFixedTS[A](rdd: RDD[(String, Seq[(A, Long)])])(implicit writer: Writes[A]): HBaseWriteRDDFixed[(A, Long)] =
    new HBaseWriteRDDFixed(rdd, pa[A])

  implicit def toHBaseRDD[A](rdd: RDD[(String, Map[String, Map[String, A]])])(implicit writer: Writes[A]): HBaseWriteRDD[A] =
    new HBaseWriteRDD(rdd, pa[A])

  implicit def toHBaseRDDT[A](rdd: RDD[(String, Map[String, Map[String, (A, Long)]])])(implicit writer: Writes[A]): HBaseWriteRDD[(A, Long)] =
    new HBaseWriteRDD(rdd, pa[A])
}

private[hbase] object HBaseWriteMethods {
  type PutAdder[A] = (Put, Array[Byte], Array[Byte], A) => Put

  // PutAdder
  def pa[A](put: Put, cf: Array[Byte], q: Array[Byte], v: A)(implicit writer: Writes[A]) = put.addColumn(cf, q, writer.write(v))
  def pa[A](put: Put, cf: Array[Byte], q: Array[Byte], v: (A, Long))(implicit writer: Writes[A]) = put.addColumn(cf, q, v._2, writer.write(v._1))
}

sealed abstract class HBaseWriteHelpers[A] {
  protected def convert(id: String, values: Map[String, Map[String, A]], put: PutAdder[A]) = {
    val p = new Put(id)
    var empty = true
    for {
      (family, content) <- values
      (key, value) <- content
    } {
      empty = false
      put(p, family, key, value)
    }

    if (empty) None else Some(new ImmutableBytesWritable, p)
  }
}

final class HBaseWriteRDDSimple[A](val rdd: RDD[(String, Map[String, A])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * Simplified form, where all values are written to the
   * same column family.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, A])]`,
   * where the first value is the rowkey and the second is a map that
   * associates column names to values.
   */
  def toHBase(table: String, family: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseWriteRDDFixed[A](val rdd: RDD[(String, Seq[A])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * Simplified form, where all values are written to the
   * same column family, and columns are fixed, so that their names can be passed as a sequence.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Seq[A])]`,
   * where the first value is the rowkey and the second is a sequence of values
   * that are associated to a sequence of headers.
   */
  def toHBase(table: String, family: String, headers: Seq[String])(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    val sc = rdd.context
    val bheaders = sc.broadcast(headers)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> Map(bheaders.value zip v: _*)), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseWriteRDD[A](val rdd: RDD[(String, Map[String, Map[String, A]])], val put: PutAdder[A]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, Map[String, A]])]`,
   * where the first value is the rowkey and the second is a nested map that associates
   * column families and column names to values.
   */
  def toHBase(table: String)(implicit config: HBaseConfig) = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}