package unicredit.spark.hbase.examples

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkContext, SparkConf }
import unicredit.spark.hbase._

/**
 * Example: bulk load to hbase of a tab separated file
 */
object ImportTsvToHfiles extends App {

  case class Config(quorum: String = "localhost",
                    rootdir: String = "hdfs://localhost:8020/hbase",
                    input_path: String = "input_tsv",
                    table: String = "output_table",
                    cfamily: String = "cf",
                    headers: List[String] = List("col1", "col2", "col3"),
                    region_size: String = "1G")

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
        case "--region_size" :: rest => parse(rest.tail, c.copy(region_size = rest.head))
        case _ => println("Wrong parameters!"); sys.exit(1)
      }
    }
  }

  /**
   * Estimates the number of table's regions based on the size of the input file
   * and the desired size of a region
   *
   * @param rdd rdd of keys which an array of start keys is extracted from
   * @param filename name of input file
   * @param regionSizeStr size of region, expressed as a number followed by B, K, M, G (e.g. "10G")
   * @param config implicit HBaseConfig instance
   * @return a sorted sequence of start keys
   */
  def getSplits(rdd: RDD[String], filename: String, regionSizeStr: String)(implicit config: HBaseConfig): Seq[String] = {

    def computeSize(sizeString: String) = {
      val sizeReg = """^([0-9]+)(B|K|M|G)?$""".r
      sizeReg findFirstMatchIn sizeString match {
        case Some(m) =>
          val num = m.group(1).toLong
          m.group(2) match {
            case "B" => num
            case "K" => num * 1024
            case "M" => num * 1024 * 1024
            case "G" => num * 1024 * 1024 * 1024
            case _ => num
          }
        case None => throw new Exception
      }
    }

    val file = new Path(filename)
    val fs = file.getFileSystem(config.get)
    val fileLength = fs.getContentSummary(file).getLength

    val regionSize = computeSize(regionSizeStr)
    val splitsCount = fileLength / regionSize + 1

    val splits = computeSplits(rdd, splitsCount.toInt)

    splits
  }

  val mainClass = this.getClass.getName.split('$')(0)

  val c = Config.parse(args.toList, Config())
  implicit lazy val hbaseconfig = HBaseConfig(c)

  val sparkConf = new SparkConf().setAppName(mainClass)
  val sc = new SparkContext(sparkConf)

  val input = sc.textFile(c.input_path)
    .map { line =>
      val fields = line.split("\t").toSeq
      (fields.head, fields.tail)
    }

  if (tableExists(c.table, c.cfamily))
    snapshot(c.table)
  else{
    val keysRdd = input.map { case (k, _) => k }
    val splits = getSplits(keysRdd, c.input_path, c.region_size)
    createTable(c.table, c.cfamily, splits)
  }

  input.loadtohbase(c.table, c.cfamily, c.headers.tail)

  sc.stop()

}