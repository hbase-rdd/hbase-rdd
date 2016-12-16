package unicredit.spark.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.json4s._
import org.json4s.jackson.JsonMethods._

trait DefaultReads {

  implicit val booleanReader = new Reads[Boolean] {
    def read(data: Array[Byte]): Boolean = Bytes.toBoolean(data)
  }

  implicit val byteArrayReader = new Reads[Array[Byte]] {
    def read(data: Array[Byte]): Array[Byte] = data
  }

  implicit val doubleReader = new Reads[Double] {
    def read(data: Array[Byte]): Double = Bytes.toDouble(data)
  }

  implicit val floatReader = new Reads[Float] {
    def read(data: Array[Byte]): Float = Bytes.toFloat(data)
  }

  implicit val intReader = new Reads[Int] {
    def read(data: Array[Byte]): Int = Bytes.toInt(data)
  }

  implicit val jsonReader = new Reads[JValue] {
    def read(data: Array[Byte]): JValue = parse(Bytes.toString(data))
  }

  implicit val longReader = new Reads[Long] {
    def read(data: Array[Byte]): Long = Bytes.toLong(data)
  }

  implicit val shortReader = new Reads[Short] {
    def read(data: Array[Byte]): Short = Bytes.toShort(data)
  }

  implicit val stringReader = new Reads[String] {
    def read(data: Array[Byte]): String = Bytes.toString(data)
  }
}
