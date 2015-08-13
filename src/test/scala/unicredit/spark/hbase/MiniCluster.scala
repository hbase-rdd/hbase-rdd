package unicredit.spark.hbase

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Suite, SuiteMixin, BeforeAndAfterAll}

trait MiniCluster extends SuiteMixin with BeforeAndAfterAll { this: Suite =>

  LogManager.getRootLogger().setLevel(Level.OFF);

  implicit def stringToBytes(s: String): Array[Byte] = Bytes.toBytes(s)
  implicit def arrayToBytes(a: Array[String]): Array[Array[Byte]] = a map (s => Bytes.toBytes(s))

  private val master = "local[4]"
  private val appName = "hbase-rdd_spark"

  val sparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  val htu: HBaseTestingUtility = HBaseTestingUtility.createLocalHTU()

  var sc: SparkContext = _

  implicit val conf = HBaseConfig(htu.getConfiguration)

  override def beforeAll() {
    htu.startMiniCluster()
  }

  override def afterAll() {
    htu.shutdownMiniCluster()
  }
}
