package unicredit.spark.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._

trait DefaultWrites {

  implicit val booleanWriter = new Writes[Boolean] {
    def write(data: Boolean) = Bytes.toBytes(data)
  }

  implicit val byteArrayWriter = new Writes[Array[Byte]] {
    def write(data: Array[Byte]) = data
  }

  implicit val doubleWriter = new Writes[Double] {
    def write(data: Double) = Bytes.toBytes(data)
  }

  implicit val floatWriter = new Writes[Float] {
    def write(data: Float) = Bytes.toBytes(data)
  }

  implicit val intWriter = new Writes[Int] {
    def write(data: Int) = Bytes.toBytes(data)
  }

  implicit val jsonWriter = new Writes[JValue] {
    def write(data: JValue) = Bytes.toBytes(compact(data))
  }

  implicit val longWriter = new Writes[Long] {
    def write(data: Long) = Bytes.toBytes(data)
  }

  implicit val shortWriter = new Writes[Short] {
    def write(data: Short) = Bytes.toBytes(data)
  }

  implicit val stringWriter = new Writes[String] {
    def write(data: String) = Bytes.toBytes(data)
  }
}
