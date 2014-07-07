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