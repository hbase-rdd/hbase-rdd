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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration

/**
 * Wrapper for HBaseConfiguration
 */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get: Configuration = HBaseConfiguration.create(defaults)
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
 * HBase API).
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

    apply(conf)
  }

  def apply(conf: { def rootdir: String; def quorum: String }): HBaseConfig = apply(
    "hbase.rootdir" -> conf.rootdir,
    "hbase.zookeeper.quorum" -> conf.quorum)
}
