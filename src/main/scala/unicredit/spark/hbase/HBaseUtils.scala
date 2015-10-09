package unicredit.spark.hbase

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{ HColumnDescriptor, TableName, HTableDescriptor }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

/**
 * Utilities for dealing with HBase tables
 */
trait HBaseUtils {

  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def arrayToBytes(a: Array[String]): Array[Array[Byte]] = a map Bytes.toBytes

  protected[hbase] def createJob(table: String, conf: Configuration) = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job
  }

  /**
   * Checks if table exists, and requires that it contains the desired column family
   *
   * @param table name of the table
   * @param family name of the column family
   *
   * @return true if table exists, false otherwise
   */
  def tableExists(table: String, family: String)(implicit config: HBaseConfig): Boolean = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(table)
      if (admin.tableExists(tableName)) {
        val families = admin.getTableDescriptor(tableName).getFamiliesKeys
        require(families.contains(family.getBytes), s"Table [$tableName] exists but column family [$family] is missing")
        true
      } else false
    } finally connection.close()
  }

  /**
   * Checks if table exists, and requires that it contains the desired column families
   *
   * @param table name of the table
   * @param families name of the column families
   *
   * @return true if table exists, false otherwise
   */
  def tableExists(table: String, families: Set[String])(implicit config: HBaseConfig): Boolean = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(table)
      if (admin.tableExists(tableName)) {
        val tfamilies = admin.getTableDescriptor(tableName).getFamiliesKeys
        for (family <- families)
          require(tfamilies.contains(family.getBytes), s"Table [$tableName] exists but column family [$family] is missing")
        true
      } else false
    }
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
   * Creates a table (if it doesn't exist already) with one or more column families
   * and made of one or more regions
   *
   * @param tableName name of the table
   * @param families list of column families
   * @param splitKeys ordered list of keys that defines region splits
   */
  def createTable(tableName: String, families: Seq[String], splitKeys: Seq[String])(implicit config: HBaseConfig): Unit = {
    val connection = ConnectionFactory.createConnection(config.get)
    try {
      val admin = connection.getAdmin
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
    } finally connection.close()
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
