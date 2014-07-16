HBase RDD
=========

![logo](https://raw.githubusercontent.com/unicredit/hbase-rdd/master/docs/logo.png)

This project allows to connect Apache Spark to HBase. Currently it is compiled with the versions of Spark and HBase available on CDH5. Other combinations of versions will be made available in the future.

Installation
------------

This guide assumes you are using SBT. Usage of similar tools like Maven or Leiningen should work with minor differences as well.

A Jar for HBase-RDD is not available yet. For the time being, you can publish it in your local repository with

    sbt publish-local

Then, you can use it inside a Spark project by adding the following dependency in sbt:

    dependencies += "unicredit" %% "hbase-rdd" % "0.2.2"

Currently, the project depends on the following artifacts:

    "org.apache.spark" %% "spark-core" % "0.9.1" % "provided",
    "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.1" % "provided",
    "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.1" % "provided",
    "org.apache.hbase" % "hbase-server" % "0.96.1.1-cdh5.0.1" % "provided",
    "org.json4s" %% "json4s-jackson" % "3.2.9" % "provided"

All dependencies appear with `provided` scope, so you will have to either have these dependencies in your project, or have the corresponding artifacts available locally in your cluster. Most of them are available in the Cloudera repositories, which you can add with the following line:

    resolvers += "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"

Usage
-----

### Premilinary

First, add the following import to get the necessary implicits:

    import unicredit.spark.hbase._

Then, you have to give configuration parameters to connect to HBase. This is done by providing an implicit instance of `unicredit.spark.hbase.HBaseConfig`. This can be done in three ways, in increasing generality.

The easiest way is to have a case class having two string members `quorum` and `rootdir`. Then, something like the following will work

    case class Config(
      quorum: String,
      rootdir: String,
      ... // Possibly other parameters
    )
    val c = Config(...)
    implicit val config = HBaseConfig(c)

In order to customize more parameters, one can provide a `Map[String, String]`, like

    implicit val config = HBaseConfig(Map(
      "hbase.rootdir" -> "...",
      "hbase.zookeeper.quorum" -> "...",
      ...
    ))

Finally, HBaseConfig can be instantiated from an existing `org.apache.hadoop.hbase.HBaseConfiguration`

    val conf: HBaseConfiguration = ...
    implicit val config = HBaseConfig(conf)

### A note on types

In HBase, every data, including tables and column names, is stored as an `Array[Byte]`. For simplicity, we assume that all table, column and column family names are actually strings.

The content of the cells, on the other hand, can have any type that can be converted to and from `Array[Byte]`. In order to do this, we have defined two traits under `unicredit.spark.hbase`:

    trait Reads[A] { def read(data: Array[Byte]): A }
    trait Writes[A] { def write(data: A): Array[Byte] }

Methods that read a type `A` from HBase will need an implicit `Reads[A]` in scope, and symmetrically methods that write to HBase require an implicit `Writes[A]`.

By default, we provide implicit readers and writers for `String`, `org.json4s.JValue` and the quite trivial `Array[Byte]`.

### Reading from HBase

Some methods are added to `SparkContext` in order to read from HBase.

If you know which columns to read, then you can use `sc.read()`. Assuming the columns `cf1:col1`, `cf1:col2` and `cf2:col3` in table `t1` are to be read, and that the content is serialized as an UTF-8 string, then one can do

    val table = "t1"
    val columns = Map(
      "cf1" -> Set("col1", "col2"),
      "cf2" -> Set("col3")
    )
    val rdd = sc.hbase[String](table, columns)

In general, `sc.hbase[A]` has a type parameter which represents the type of the content of the cells, and it returns a `RDD[(String, Map[String, Map[String, A]])]`. Each element of the resulting RDD is a key/value pair, where the key is the rowkey from HBase and the value is a nested map which associates column family and column to the value. Missing columns are omitted from the map, so for instance one can project the above on the `col2` column doing something like

    rdd.flatMap({ case (k, v) =>
      v("cf1") get "col2" map { col =>
        k -> col
      }
      // or equivalently
      // Try(k -> v("cf1")("col2")).toOption
    })

A second possibility is to get the whole column families. This can be useful if you do not know in advance which will be the column names. You can do this with the method `sc.hbaseFull[A]`, like

    val table = "t1"
    val famlies = Set("cf1", "cf2")
    val rdd = sc.hbase[String](table, columns)

The output, like `sc.hbase[A]`, is a `RDD[(String, Map[String, Map[String, A]])]`.

Finally, there is a lower level access to the raw `org.apache.hadoop.hbase.client.Result` instances. For this, just do

    val table = "t1"
    val rdd = sc.hbaseRaw(table)

The return value of `sc.hbaseRaw` (note that in this case there is no type parameter) is a `RDD[(String, Result)]`. The first element is the rowkey, while the second one is an instance of `org.apache.hadoop.hbase.client.Result`, so you can use the raw HBase API to query it.


### Writing to HBase

To be continued...