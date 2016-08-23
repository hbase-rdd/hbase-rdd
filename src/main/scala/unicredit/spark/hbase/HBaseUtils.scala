package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Utilities for dealing with HBase tables
 */
trait HBaseUtils {

  protected[hbase] def createJob(table: String, conf: Configuration) = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job
  }

  object Admin {
    def apply()(implicit config: HBaseConfig) = new Admin(ConnectionFactory.createConnection(config.get))
  }

  class Admin(connection: Connection) {
    /**
     * Closes the connection to HBase. Must the Admin instance is no more needed
     */
    def close = connection.close()

    /**
     * Checks if table exists, and requires that it contains the desired column family
     *
     * @param tableName name of the table
     * @param family name of the column family
     *
     * @return true if table exists, false otherwise
     */
    def tableExists[Q](tableName: String, family: Q)(implicit wq: Writes[Q]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      val bf = wq.write(family)
      if (admin.tableExists(table)) {
        val families = admin.getTableDescriptor(table).getFamiliesKeys
        require(families.contains(bf), s"Table [$table] exists but column family [$family] is missing")
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
    def tableExists[Q](tableName: String, families: Set[Q])(implicit wq: Writes[Q]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val tfamilies = admin.getTableDescriptor(table).getFamiliesKeys
        for (family <- families)
          require(tfamilies.contains(wq.write(family)), s"Table [$table] exists but column family [$family] is missing")
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
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      admin.snapshot(snapshotName, tableDescriptor.getTableName)
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
    def createTable[K, Q](tableName: String, families: Set[Q], splitKeys: Seq[K])(implicit wk: Writes[K], wq: Writes[Q]): Admin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (!admin.isTableAvailable(table)) {
        val tableDescriptor = new HTableDescriptor(table)
        families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(wq.write(f))) }
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
    def createTable[Q: Writes](tableName: String, families: Set[Q]): Admin =
      createTable[String, Q](tableName, families, Nil)

    /**
     * Creates a table (if it doesn't exist already) with one or more column families
     *
     * @param tableName name of table
     * @param families list of one or more column families
     */
    def createTable[Q: Writes](tableName: String, families: Q*): Admin =
      createTable[String, Q](tableName, families.toSet, Nil)

    /**
     * Creates a table (if it doesn't exist already) with a column family and made of one or more regions
     *
     * @param tableName name of table
     * @param family name of column family
     * @param splitKeys ordered list of keys that defines region splits
     */
    def createTable[K: Writes, Q: Writes](tableName: String, family: Q, splitKeys: Seq[K]): Admin =
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
