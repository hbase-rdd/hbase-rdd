package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.rdd.RDD

/**
 * Utilities for dealing with HBase tables
 */
trait HBaseUtils {
  /**
   * Checks if table exists, and requires that it contains the desired column family
   *
   * @param tableName name of the table
   * @param cFamily name of the column family
   *
   * @return true if table exists, false otherwise
   */
  def tableExists(tableName: String, cFamily: String)(implicit config: HBaseConfig): Boolean = {
    val admin = new HBaseAdmin(config.get)
    if (admin.tableExists(tableName)) {
      val families = admin.getTableDescriptor(tableName.getBytes).getFamiliesKeys
      require(families.contains(cFamily.getBytes), s"Table [$tableName] exists but column family [$cFamily] is missing")
      true
    } else false
  }

  /**
   * Takes a snapshot of the table, the snapshot's name has format "tableName_yyyyMMddHHmmss"
   *
   * @param tableName name of the table
   */
  def snapshot(tableName: String)(implicit config: HBaseConfig): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val suffix = sdf.format(Calendar.getInstance().getTime)
    snapshot(tableName, s"${tableName}_$suffix")
  }

  /**
   * Takes a snapshot of the table
   *
   * @param tableName name of the table
   * @param snapshotName name of the snapshot
   */
  def snapshot(tableName: String, snapshotName: String)(implicit config: HBaseConfig): Unit = {
    val admin = new HBaseAdmin(config.get)
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    val tName = tableDescriptor.getTableName
    admin.snapshot(snapshotName, tName)
  }

  /**
   * Creates a table (if it doesn't exist already) with one or more column families
   * and made of one or more regions
   *
   * @param tableName name of the table
   * @param families list of column families
   * @param splitKeys ordered list of keys that defines region splits
   */
  def createTable(tableName: String, families: Seq[String], splitKeys: Seq[String])(implicit config: HBaseConfig): Unit = {
    val admin = new HBaseAdmin(config.get)
    val table = TableName.valueOf(tableName)
    if (!admin.isTableAvailable(table)) {
      val tableDescriptor = new HTableDescriptor(table)
      families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(f)) }
      if (splitKeys.isEmpty)
        admin.createTable(tableDescriptor)
      else {
        val splitKeysBytes = splitKeys.map(Bytes.toBytes).toArray
        admin.createTable(tableDescriptor, splitKeysBytes)
      }
    }
  }

  /**
   * Creates a table (if it doesn't exist already) with one or more column families
   *
   * @param tableName name of the table
   * @param families list of one or more column families
   */
  def createTable(tableName: String, families: String*)(implicit config: HBaseConfig): Unit =
    createTable(tableName, families, Seq.empty)

  /**
   * Creates a table (if it doesn't exist already) with a column family and made of one or more regions
   *
   * @param tableName name of the table
   * @param family name of the column family
   * @param splitKeys ordered list of keys that defines region splits
   */
  def createTable(tableName: String, family: String, splitKeys: Seq[String])(implicit config: HBaseConfig): Unit =
    createTable(tableName, Seq(family), splitKeys)

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
  def computeSplits(rdd: RDD[String], regionsCount: Int): Seq[String] = {
    rdd.sortBy(s => s, numPartitions = regionsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }
}
