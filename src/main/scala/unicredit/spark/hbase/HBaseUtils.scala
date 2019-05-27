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

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, ConnectionFactory, TableDescriptorBuilder}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Utilities for dealing with HBase tables
 */
trait HBaseUtils {

  protected[hbase] def createJob(table: String, conf: Configuration): Job = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[_]])
    job
  }

  object Admin {
    def apply()(implicit config: HBaseConfig) = new Admin(ConnectionFactory.createConnection(config.get))
  }

  class Admin(connection: Connection) {
    /**
     * Closes the connection to HBase. Must the Admin instance is no more needed
     */
    def close(): Unit = connection.close()

    /**
     * Checks if table exists, and requires that it contains the desired column family
     *
     * @param tableName name of the table
     * @param family name of the column family
     *
     * @return true if table exists, false otherwise
     */
    def tableExists(tableName: String, family: String)(implicit wq: Writes[String]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val families = admin.getDescriptor(table).getColumnFamilies
        require(families.exists(_.getNameAsString == family), s"Table [$table] exists but column family [$family] is missing")
        true
      } else false
    }

    /**
     * Checks if table exists, and requires that it contains the desired column families
     *
     * @param tableName name of table
     * @param families set of column families
     *
     * @return true if table exists, false otherwise
     */
    def tableExists(tableName: String, families: Set[String])(implicit wq: Writes[String]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val tfamilies = admin.getDescriptor(table).getColumnFamilies
        for (family <- families)
          require(tfamilies.exists(_.getNameAsString == family), s"Table [$table] exists but column family [$family] is missing")
        true
      } else false
    }

    /**
     * Takes a snapshot of the table, the snapshot's name has format "tableName_yyyyMMddHHmmss"
     *
     * @param tableName name of table
     */
    def snapshot(tableName: String): Admin = {
      val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
      val suffix = sdf.format(Calendar.getInstance().getTime)
      snapshot(tableName, s"${tableName}_$suffix")
      this
    }

    /**
     * Takes a snapshot of the table
     *
     * @param tableName name of table
     * @param snapshotName name of snapshot
     */
    def snapshot(tableName: String, snapshotName: String): Admin = {
      val admin = connection.getAdmin
      admin.snapshot(snapshotName, TableName.valueOf(tableName))
      this
    }

    /**
     * Creates a table (if it doesn't exist already) with one or more column families
     * and made of one or more regions
     *
     * @param tableName name of table
     * @param families set of column families
     * @param splitKeys ordered list of keys that defines region splits
     */
    def createTable[K](tableName: String, families: Set[String], splitKeys: Seq[K])(implicit wk: Writes[K], wq: Writes[String]): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (!admin.isTableAvailable(table)) {
        val tableDescriptor = TableDescriptorBuilder
          .newBuilder(table)
          .setColumnFamilies(families.map(f => ColumnFamilyDescriptorBuilder.newBuilder(wq.write(f)).build()).asJava)
          .build()
        if (splitKeys.isEmpty)
          admin.createTable(tableDescriptor)
        else {
          val splitKeysBytes = splitKeys.map(wk.write).toArray
          admin.createTable(tableDescriptor, splitKeysBytes)
        }
      }
      this
    }

    /**
      * Creates a table (if it doesn't exist already) with one or more column families
      * and made of one or more regions
      *
      * @param tableName name of table
      * @param families set of column families
      */
    def createTable(tableName: String, families: Set[String]): Admin =
      createTable[String](tableName, families, Nil)

    /**
     * Creates a table (if it doesn't exist already) with one or more column families
     *
     * @param tableName name of table
     * @param families list of one or more column families
     */
    def createTable(tableName: String, families: String*): Admin =
      createTable[String](tableName, families.toSet, Nil)

    /**
     * Creates a table (if it doesn't exist already) with a column family and made of one or more regions
     *
     * @param tableName name of table
     * @param family name of column family
     * @param splitKeys ordered list of keys that defines region splits
     */
    def createTable[K: Writes](tableName: String, family: String, splitKeys: Seq[K]): Admin =
      createTable(tableName, Set(family), splitKeys)

    /**
      * Disables a table
      *
      * @param tableName name of table
      */
    def disableTable(tableName: String): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.disableTable(table)
      this
    }

    /**
      * Deletes a table
      *
      * @param tableName name of table
      */
    def deleteTable(tableName: String): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.deleteTable(table)
      this
    }

    /**
      * Truncates a table
      *
      * @param tableName name of table
      * @param preserveSplits true if region splits should be preserved
      */
    def truncateTable(tableName: String, preserveSplits: Boolean): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.truncateTable(table, preserveSplits)
      this
    }
  }

  /**
   * Given a RDD of keys and the number of requested table's regions, returns an array
   * of keys that are start keys of the table's regions. The array length is
   * ''regionsCount-1'' since the start key of the first region is not needed
   * (since it does not determine a split)
   *
   * @param rdd RDD of strings representing row keys
   * @param regionsCount number of regions
   *
   * @return a sorted sequence of start keys
   */
  def computeSplits[K: ClassTag](rdd: RDD[K], regionsCount: Int)(implicit ord: Ordering[K]): Seq[K] = {
    rdd.sortBy(s => s, numPartitions = regionsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }
}
