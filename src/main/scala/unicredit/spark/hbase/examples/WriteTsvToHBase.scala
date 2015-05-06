package unicredit.spark.hbase.examples

import org.apache.spark.{ SparkContext, SparkConf }
import unicredit.spark.hbase._

/**
 * Example: bulk load to hbase of a tab separated file
 */
object WriteTsvToHBase extends App {

  case class Config(quorum: String = "localhost",
    rootdir: String = "hdfs://localhost:8020/hbase",
    input_path: String = "input_tsv",
    table: String = "output_table",
    cfamily: String = "cf",
    headers: List[String] = List("col1", "col2", "col3"))

  object Config {
    /**
     * Very basic argument parser
     *
     * @param args list of command line arguments
     * @param c case class to be initialized with parameters
     * @return initialized case class
     */
    def parse(args: List[String], c: Config): Config = {
      if (args.length < 2) c
      else args match {
        case "--quorum" :: rest => parse(rest.tail, c.copy(quorum = rest.head))
        case "--rootdir" :: rest => parse(rest.tail, c.copy(rootdir = rest.head))
        case "--input_path" :: rest => parse(rest.tail, c.copy(input_path = rest.head))
        case "--table" :: rest => parse(rest.tail, c.copy(table = rest.head))
        case "--cfamily" :: rest => parse(rest.tail, c.copy(cfamily = rest.head))
        case "--headers" :: rest => parse(rest.tail, c.copy(headers = rest.head.split(",").toList))
        case _ => println("Wrong parameters!"); sys.exit(1)
      }
    }
  }

  val mainClass = this.getClass.getName.split('$')(0)

  val c = Config.parse(args.toList, Config())
  implicit lazy val hbaseconfig = HBaseConfig(c)

  val sparkConf = new SparkConf().setAppName(mainClass)
  val sc = new SparkContext(sparkConf)

  val input = sc.textFile(c.input_path)
    .map { line =>
      val fields = line.split("\t").toSeq

      // add a timestamp=1L to values
      val values = fields.tail.map(v => (v, 1L))

      (fields.head, values)
    }

  input.tohbase(c.table, c.cfamily, c.headers.tail)

  sc.stop()
}