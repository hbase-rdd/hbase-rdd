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

/**
 * An instance of Reads[A] testifies that Array[Byte]
 * can be converted to A.
 */
trait Reads[A] extends Serializable {
  def read(data: Array[Byte]): A
}

/**
 * An instance of Writes[A] testifies that A
 * can be converted to Array[Byte].
 */
trait Writes[A] extends Serializable {
  def write(data: A): Array[Byte]
}

trait Formats[A] extends Reads[A] with Writes[A]