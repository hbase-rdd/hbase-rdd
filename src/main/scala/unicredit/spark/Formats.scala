package unicredit.spark.hbase


trait Reads[A] extends Serializable {
  def read(data: Array[Byte]): A
}

trait Writes[A] extends Serializable {
  def write(data: A): Array[Byte]
}

trait Formats[A] extends Reads[A] with Writes[A]