package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HColumnDescriptor, TableName, HTableDescriptor }
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.rdd.RDD

/**
 * Utilities for dealing with HBase tables
 */
trait HBaseUtils {

  /**
   * Checks if table exists, and requires that it contains the desired column family
   *
   * @param table name of the table
   * @param cFamily name of the column family
   *
   * @return true if table exists, false otherwise
   */
  def tableExists(table: String, cFamily: String)(implicit config: HBaseConfig): Boolean = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(table)
      if (admin.tableExists(tableName)) {
        val families = admin.getTableDescriptor(tableName).getFamiliesKeys
        require(families.contains(cFamily.getBytes), s"Table [$tableName] exists but column family [$cFamily] is missing")
        true
      } else false
    } finally connection.close()
  }

  /**
   * Takes a snapshot of the table, the snapshot's name has format "tableName_yyyyMMddHHmmss"
   *
   * @param table name of the table
   */
  def snapshot(table: String)(implicit config: HBaseConfig): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val suffix = sdf.format(Calendar.getInstance().getTime)
    snapshot(table, s"${table}_$suffix")
  }

  /**
   * Takes a snapshot of the table
   *
   * @param table name of the table
   * @param snapshotName name of the snapshot
   */
  def snapshot(table: String, snapshotName: String)(implicit config: HBaseConfig): Unit = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(table))
      val tName = tableDescriptor.getTableName
      admin.snapshot(snapshotName, tName)
    } finally connection.close()
  }

  /**
   * Creates a table with a column family and made of one or more regions
   *
   * @param table name of the table
   * @param cFamily name of the column family
   * @param splitKeys ordered list of keys that defines region splits
   */
  def createTable(table: String, cFamily: String, splitKeys: Seq[String])(implicit config: HBaseConfig): Unit = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(table))
      tableDescriptor.addFamily(new HColumnDescriptor(cFamily))
      if (splitKeys.isEmpty)
        admin.createTable(tableDescriptor)
      else {
        val splitKeysBytes = splitKeys.map(Bytes.toBytes).toArray
        admin.createTable(tableDescriptor, splitKeysBytes)
      }
    } finally connection.close()
  }

  /**
   * Creates a table (if it doesn't exist already) with one or more column families
   *
   * @param table name of the table
   * @param families list of column families
   */
  def createTable(table: String, families: List[String])(implicit config: HBaseConfig): Unit = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(table)
      if (!admin.isTableAvailable(tableName)) {
        val tableDescriptor = new HTableDescriptor(tableName)

        families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(f)) }
        admin.createTable(tableDescriptor)
      }
    } finally connection.close()
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
  def computeSplits(rdd: RDD[String], regionsCount: Int): Seq[String] = {
    rdd.sortBy(s => s, numPartitions = regionsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }
}
