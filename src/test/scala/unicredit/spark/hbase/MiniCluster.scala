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
