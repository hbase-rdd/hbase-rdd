package unicredit.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

class HBaseConfig(val options: Map[String, String]) extends Serializable {
  def apply(conf: Configuration): Unit =
    for ((key, value) <- options) { conf.set(key, value) }
}

object HBaseConfig {
  def apply(options: (String, String)*): HBaseConfig = new HBaseConfig(options.toMap)

  def apply(config: { def rootdir: String; def quorum: String }): HBaseConfig = apply(
    "hbase.rootdir" -> config.rootdir,
    "hbase.zookeeper.quorum" -> config.quorum
  )
}
