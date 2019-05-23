package unicredit.spark.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite, SuiteMixin}

trait MiniCluster extends SuiteMixin with BeforeAndAfterAll { this: Suite =>

  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def arrayToBytes(a: Array[String]): Array[Array[Byte]] = a map Bytes.toBytes

  private val master = "local[4]"
  private val appName = "hbase-rdd_spark"

  val sparkConf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  val htu = new HBaseTestingUtility()

  var sc: SparkContext = _

  implicit val conf = HBaseConfig(htu.getConfiguration)

  override def beforeAll() {
    htu.startMiniCluster()
  }

  override def afterAll() {
    htu.shutdownMiniCluster()
  }
}
