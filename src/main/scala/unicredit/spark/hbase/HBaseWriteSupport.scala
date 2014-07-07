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

import org.apache.hadoop.hbase.client.{ Put, HBaseAdmin }
import org.apache.hadoop.hbase.{ HTableDescriptor, HColumnDescriptor, TableName }
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


/**
 * Adds implicit methods to RDD[(String, Map[String, A])] to write
 * to HBase sources.
 */
trait HBaseWriteSupport {
  implicit def toHBaseRDD[A](rdd: RDD[(String, Map[String, A])])
    (implicit writer: Writes[A]) = new HBaseRDD(rdd, writer)

  implicit val stringWriter = new Writes[String] {
    def write(data: String) = data.getBytes
  }

  implicit val jsonWriter = new Writes[JValue] {
    def write(data: JValue) = compact(data).getBytes
  }
}

final class HBaseRDD[A](val rdd: RDD[(String, Map[String, A])], val writer: Writes[A]) extends Serializable {
  private def convert(id: String, values: Map[String, A], family: String) = {
    def bytes(s: String) = Bytes.toBytes(s)

    val put = new Put(bytes(id))
    for ((key, value) <- values) {
      put.add(bytes(family), bytes(key), writer.write(value))
    }
    (new ImmutableBytesWritable, put)
  }

  private def createTable(table: String, family: String, admin: HBaseAdmin) = {
    if (!admin.isTableAvailable(table)) {
      val tableName = TableName.valueOf(table)
      val tableDescriptor = new HTableDescriptor(tableName)

      tableDescriptor.addFamily(new HColumnDescriptor(family))
      admin.createTable(tableDescriptor)
    }
  }

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

    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    createTable(table, family, new HBaseAdmin(conf))

    val jobConf = new JobConf(conf, getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    rdd.map({ case (k, v) => convert(k, v, family) }).saveAsHadoopDataset(jobConf)
  }
}