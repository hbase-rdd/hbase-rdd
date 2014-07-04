package unicredit.spark

import org.apache.spark.{ SparkContext, SparkConf }

trait Job {
  def name: String

  lazy val sparkConf = new SparkConf().setAppName(name)
  lazy val sc = new SparkContext(sparkConf)
}
