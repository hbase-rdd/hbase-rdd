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
