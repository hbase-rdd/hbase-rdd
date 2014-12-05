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

import org.apache.hadoop.hbase.client.{ Put, Mutation, HBaseAdmin }
import org.apache.hadoop.hbase.{ HTableDescriptor, HColumnDescriptor, TableName }
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.util.Bytes

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


/**
 * Adds implicit methods to `RDD[(String, Map[String, A])]` or
 * `RDD[(String, Map[String, A])]` to write to HBase sources.
 */
trait HBaseWriteSupport {
  implicit def toHBaseRDDSimple[A](rdd: RDD[(String, Map[String, A])]) = new HBaseRDDSimple(rdd)
  implicit def toHBaseRDD[A](rdd: RDD[(String, Map[String, Map[String, A]])]) = new HBaseRDD(rdd)

  implicit val byteArrayWriter = new Writes[Array[Byte]] {
    def write(data: Array[Byte]) = data
  }

  implicit val stringWriter = new Writes[String] {
    def write(data: String) = data.getBytes
  }

  implicit val jsonWriter = new Writes[JValue] {
    def write(data: JValue) = compact(data).getBytes
  }
}

sealed abstract class HBaseWriteHelpers[A] {
  protected def convert(id: String, values: Map[String, Map[String, A]], writer: Writes[A]) = {
    def bytes(s: String) = Bytes.toBytes(s)

    val put = new Put(bytes(id))
    var empty = true
    for {
      (family, content) <- values
      (key, value) <- content
    } {
      empty = false
      put.add(bytes(family), bytes(key), writer.write(value))
    }

    if (empty) None else Some(new ImmutableBytesWritable, put)
  }

  protected def createTable(table: String, families: List[String], admin: HBaseAdmin) = {
    if (!admin.isTableAvailable(table)) {
      val tableName = TableName.valueOf(table)
      val tableDescriptor = new HTableDescriptor(tableName)

      families foreach  { f => tableDescriptor.addFamily(new HColumnDescriptor(f)) }
      admin.createTable(tableDescriptor)
    }
  }
}

final class HBaseRDDSimple[A](val rdd: RDD[(String, Map[String, A])]) extends HBaseWriteHelpers[A] with Serializable {
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
  def tohbase(table: String, family: String)
    (implicit config: HBaseConfig, writer: Writes[A]) = {
      val conf = config.get

      conf.set(TableOutputFormat.OUTPUT_TABLE, table)
      createTable(table, List(family), new HBaseAdmin(conf))

      val job = new Job(conf, this.getClass.getName.split('$')(0))
      job.setOutputFormatClass(classOf[TableOutputFormat[String]])

      rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), writer) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
}

final class HBaseRDD[A](val rdd: RDD[(String, Map[String, Map[String, A]])]) extends HBaseWriteHelpers[A] with Serializable {
  /**
   * Writes the underlying RDD to HBase.
   *
   * The RDD is assumed to be an instance of `RDD[(String, Map[String, Map[String, A]])]`,
   * where the first value is the rowkey and the second is a nested map that associates
   * column families and column names to values.
   */
  def tohbase(table: String)
    (implicit config: HBaseConfig, writer: Writes[A]) = {
      val conf = config.get
      conf.set(TableOutputFormat.OUTPUT_TABLE, table)

      val job = new Job(conf, this.getClass.getName.split('$')(0))
      job.setOutputFormatClass(classOf[TableOutputFormat[String]])

      rdd.flatMap({ case (k, v) => convert(k, v, writer) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    }
}