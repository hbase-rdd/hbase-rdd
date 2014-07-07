package unicredit.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD


/**
 * Wrapper for HBaseConfiguration
 */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get = HBaseConfiguration.create(defaults)
}

/**
 * Factories to generate HBaseConfig instances.
 *
 * We can generate an HBaseConfig
 * - from an existing HBaseConfiguration
 * - from a sequence of String key/value pairs
 * - from an existing object with a `rootdir` and `quorum` members
 *
 * The two latter cases are provided for simplicity
 * (ideally a client should not have to deal with the native
 * HBase API). In these cases, we also add two default values.
 *
 * The last constructor contains the minimum information to
 * be able to read and write to the HBase cluster. It can be used
 * in tandem with a case class containing job configuration.
 */
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
