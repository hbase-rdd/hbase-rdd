HBase RDD
=========

![logo](https://raw.githubusercontent.com/unicredit/hbase-rdd/master/docs/logo.png)

This project allows to connect Apache Spark to HBase. Currently it is compiled with Scala 2.10, using the versions of Spark and HBase available on CDH5.1. Version `0.2.2-SNAPSHOT` of this project works on CDH5.0. Other combinations of versions will be made available in the future.

Installation
------------

This guide assumes you are using SBT. Usage of similar tools like Maven or Leiningen should work with minor differences as well.

A Jar for the preliminary version is available on the Sonatype snapshots repository. You can add the repository with

    resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

Then, you can add the following dependency in sbt:

    dependencies += "eu.unicredit" %% "hbase-rdd" % "0.3.0-SNAPSHOT"

Currently, the project depends on the following artifacts:

    "org.apache.spark" %% "spark-core" % "1.0.2" % "provided",
    "org.apache.hbase" % "hbase-common" % "0.98.1-cdh5.1.3" % "provided",
    "org.apache.hbase" % "hbase-client" % "0.98.1-cdh5.1.3" % "provided",
    "org.apache.hbase" % "hbase-server" % "0.98.1-cdh5.1.3" % "provided",
    "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided"

All dependencies appear with `provided` scope, so you will have to either have these dependencies in your project, or have the corresponding artifacts available locally in your cluster. Most of them are available in the Cloudera repositories, which you can add with the following line:

    resolvers += "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"

Usage
-----

### Preliminary

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

Finally, HBaseConfig can be instantiated from an existing `org.apache.hadoop.conf.Configuration`

    val conf: Configuration = ...
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
    val families = Set("cf1", "cf2")
    val rdd = sc.hbase[String](table, families)

The output, like `sc.hbase[A]`, is a `RDD[(String, Map[String, Map[String, A]])]`.

Finally, there is a lower level access to the raw `org.apache.hadoop.hbase.client.Result` instances. For this, just do

    val table = "t1"
    val rdd = sc.hbaseRaw(table)

The return value of `sc.hbaseRaw` (note that in this case there is no type parameter) is a `RDD[(String, Result)]`. The first element is the rowkey, while the second one is an instance of `org.apache.hadoop.hbase.client.Result`, so you can use the raw HBase API to query it.


### Writing to HBase

In order to write to HBase, some methods are added on certain types of RDD.

The first one is parallel to the way you read from HBase. Assume you have an `RDD[(String, Map[String, Map[String, A]])]` and there is a `Writes[A]` in scope. Then you can write to HBase with the method `tohbase`, like

    val table = "t1"
    val rdd: RDD[(String, Map[String, Map[String, A]])] = ...
    rdd.tohbase(table)

A simplified form is available in the case that one only needs to write on a single column family. Then a similar method is available on `RDD[(String, Map[String, A])]`, which can be used as follows

    val table = "t1"
    val cf = "cf1"
    val rdd: RDD[(String, Map[String, A])] = ...
    rdd.tohbase(table, cf)


### Bulk load to HBase, using HFiles

In case of massive writing to HBase, writing Puts objects directly into the table can be inefficient and can cause HBase to be unresponsive (e.g. it can trigger region splitting).
A better approach is to create HFiles instead, and than call LoadIncrementalHFiles job to move them to HBase's file system. Unfortunately this approach is quite cumbersome, as it implies the following steps:

1. Make sure the table exists and has region splits so that rows are evenly distributed into regions (for better performance).

2. Implement and execute a map (and reduce) job to write ordered Put or KeyValue objects to HFile files, using HFileOutputFormat2 output format. The reduce phase is configured behind the scenes with a call to HFileOutputFormat2.configureIncrementalLoad.

3. Execute LoadIncrementalHFiles job to move HFile files to HBase's file system.

4. Cleanup temporary files and folders

Now you can perform steps 2 to 4 with a call to `loadToHBase`, like

    val table = "t1"
    val cf = "cf1"
    val rdd: RDD[(K, Map[C, V])] = ...
    rdd.loadToHBase(table, cf)

or, if you have a fixed set of columns, like

    val table = "t1"
    val cf = "cf1"
    val headers: Seq[String] = ...
    val rdd: RDD[(K, Seq[V])] = ...
    rdd.loadToHBase(table, cf, headers)

where `headers` are column names for `Seq[V]` values.
The only limitation is that you can work with only one column family.

But what about step 1? For this, `prepareTable` comes to the rescue. If your input data is a tsv file on Hdfs, you can write

    if (prepareTable(table, cf, input_path, region_size, header, takeSnapshot = false)) {

      ...

      rdd.loadToHBase(table, cf, headers)
    }

where `input_path` is the path to the file, `region_size` is the desired size of regions, represented as a number followed by B, K, M, G ("10G" is a good value), `header` is the name of the row key field (for tsv with headers, it can be null otherwise), set `takeSnapshot` to `true` if you want to take a snapshot of the existing table before loading new data.
More generally, you can use instead

    if (prepareTable(table, cf, keys, splitsCount, takeSnapshot = false)) {

      ...

      rdd.loadToHBase(table, cf, headers)
    }

where `keys` is an `RDD[String]` containing all the row keys and `splitCount` the number of splits that you want for a new table (that you must compute in some way) and it is not relevant if the table exists.

`prepareTable` verifies that, if the table exists, it contains the desired column family (returns false otherwise), and optionally takes a snapshot of the table.
If table does not exist, it computes a list of split keys and creates a new table with these splits and the desired column family.


API stability
-------------

The API described above should be considered unstable. The published, non-snapshot version of HBase-RDD may contain a slightly different API, based on comments received for the first version.
