package unicredit.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD


class HBaseConfig(defaults: Configuration) extends Serializable {
  def get = HBaseConfiguration.create(defaults)
}

object HBaseConfig {
  def apply(conf: Configuration): HBaseConfig = new HBaseConfig(conf)

  def apply(options: (String, String)*): HBaseConfig = {
    val conf = HBaseConfiguration.create

    for ((key, value) <- options) { conf.set(key, value) }
    conf.setBoolean("hbase.cluster.distributed", true)
    conf.setInt("hbase.client.scanner.caching", 10000)

    apply(conf)
  }

  def apply(conf: { def rootdir: String; def quorum: String }): HBaseConfig = apply(
    "hbase.rootdir" -> conf.rootdir,
    "hbase.zookeeper.quorum" -> conf.quorum
  )
}
