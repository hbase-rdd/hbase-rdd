package unicredit.spark

import org.apache.spark.SparkContext

trait TsvReadSupport {
  implicit def toTsvRDD(sc: SparkContext) = new TsvRDD(sc)
}

final class TsvRDD(val sc: SparkContext) extends Serializable {
  def tsv(path: String, fields: Seq[String], separator: Char = '\t') = sc.textFile(path) map { line =>
    val contents = line.split(separator)
    val data = contents ++ Array.fill(fields.length - contents.length)("")

    (fields, data).zipped.toMap
  }
}

