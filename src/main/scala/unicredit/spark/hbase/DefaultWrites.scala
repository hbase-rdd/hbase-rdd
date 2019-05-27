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

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._

trait DefaultWrites {

  implicit val booleanWriter = new Writes[Boolean] {
    def write(data: Boolean): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val byteArrayWriter = new Writes[Array[Byte]] {
    def write(data: Array[Byte]): Array[Byte] = data
  }

  implicit val doubleWriter = new Writes[Double] {
    def write(data: Double): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val floatWriter = new Writes[Float] {
    def write(data: Float): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val intWriter = new Writes[Int] {
    def write(data: Int): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val jsonWriter = new Writes[JValue] {
    def write(data: JValue): Array[Byte] = Bytes.toBytes(compact(data))
  }

  implicit val longWriter = new Writes[Long] {
    def write(data: Long): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val shortWriter = new Writes[Short] {
    def write(data: Short): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val stringWriter = new Writes[String] {
    def write(data: String): Array[Byte] = Bytes.toBytes(data)
  }
}
