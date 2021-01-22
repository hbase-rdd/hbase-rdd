HBase RDD
=========

![logo](https://raw.githubusercontent.com/unicredit/hbase-rdd/master/docs/logo.png)

![Maven central](https://maven-badges.herokuapp.com/maven-central/eu.unicredit/hbase-rdd_2.12/badge.svg?color=orange)

[![Latest version](https://index.scala-lang.org/unicredit/hbase-rdd/hbase-rdd/latest.svg?color=orange)](https://index.scala-lang.org/unicredit/hbase-rdd/hbase-rdd)

This project allows to connect Apache Spark to HBase. Currently it is compiled with Scala 2.11 and 2.12, using the versions of Spark and HBase available on CDH6.2.

Here is a list of available versions for different CDH releases:

|Version|Flavour|Spark|HBase          |Scala    |
|:-----:|:-----:|:---:|:-------------:|:-------:|
|0.9.1  |CDH 6.3|2.4.7|2.1.0-cdh6.3.4 |2.11/2.12|
|0.9.0  |CDH 6.2|2.4.3|2.1.0-cdh6.2.0 |2.11/2.12|
|0.8.0  |CDH 5.5|1.5.0|1.0.0-cdh5.5.2 |2.10/2.11|
|0.6.0  |CDH 5.3|1.2.0|0.98.6-cdh5.3.1|2.10     |
|0.4.0  |CDH 5.1|1.0.2|0.98.1-cdh5.1.3|2.10     |

Table of contents
-----------------

- [Installation](#installation)
- [Preliminary](#preliminary)
- [A note on types](#a-note-on-types)
- [Reading from HBase](#reading-from-hbase)
- [Writing to HBase](#writing-to-hbase)
- [Deleting from HBase](#deleting-from-hbase)
- [Bulk loading to HBase](#bulk-load-to-hbase-using-hfiles)
- [Example project](https://github.com/unicredit/hbase-rdd-examples)

Installation
------------

This guide assumes you are using SBT. Usage of similar tools like Maven or Leiningen should work with minor differences as well.

HBase RDD can be added as a dependency in sbt with:

    dependencies += "eu.unicredit" %% "hbase-rdd" % "0.9.1"

Currently, the project depends on the following artifacts:

    "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
    "org.apache.hbase" % "hbase-common" % "2.1.0-cdh6.3.4" % "provided",
    "org.apache.hbase" % "hbase-mapreduce" % "2.1.0-cdh6.3.4" % "provided",
    "org.apache.hbase" % "hbase-server" % "2.1.0-cdh6.3.4" % "provided",


All dependencies appear with `provided` scope, so you will have to either have these dependencies in your project, or have the corresponding artifacts available locally in your cluster. Most of them are available in the Cloudera repositories, which you can add with the following line:

    resolvers ++= Seq(
      "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
    )

Usage
-----

### Preliminary

First, add the following import to get the necessary implicits:

    import unicredit.spark.hbase._

Then, you have to give configuration parameters to connect to HBase. This is done by providing an implicit instance of `unicredit.spark.hbase.HBaseConfig`. This can be done in a few ways, in increasing generality.

#### With `hbase-site.xml`

If you happen to have on the classpath `hbase-site.xml` with the right configuration parameters, you can just do

    implicit val config = HBaseConfig()

Otherwise, you will have to configure HBase RDD programmatically.

#### With a case class

The easiest way is to have a case class having two string members `quorum` and `rootdir`. Then, something like the following will work

    case class Config(
      quorum: String,
      rootdir: String,
      ... // Possibly other parameters
    )
    val c = Config(...)
    implicit val config = HBaseConfig(c)

#### With a map

In order to customize more parameters, one can provide a sequence of `(String, String)`, like

    implicit val config = HBaseConfig(
      "hbase.rootdir" -> "...",
      "hbase.zookeeper.quorum" -> "...",
      ...
    )

#### With a Hadoop configuration object

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

If you know which columns to read, then you can use `sc.hbase()`. Assuming the columns `cf1:col1`, `cf1:col2` and `cf2:col3` in table `t1` are to be read, and that the content is serialized as an UTF-8 string, then one can do

    val table = "t1"
    val columns = Map(
      "cf1" -> Set("col1", "col2"),
      "cf2" -> Set("col3")
    )
    val rdd = sc.hbase[String](table, columns)

In general, `sc.hbase[K, Q, V]` has type parameters which represent the types of row key, qualifier and content of the cells, and it returns a `RDD[(K, Map[String, Map[Q, V]])]`. Each element of the resulting RDD is a key/value pair, where the key is the rowkey from HBase and the value is a nested map which associates column family and column to the value. Missing columns are omitted from the map, so for instance one can project the above on the `col2` column doing something like

    rdd.flatMap({ case (k, v) =>
      v("cf1") get "col2" map { col =>
        k -> col
      }
      // or equivalently
      // Try(k -> v("cf1")("col2")).toOption
    })

You can also read with `sc.hbase[K, V](table, columns)` where only the type parameters for row key and value are explicit, while qualifier is a String, or `sc.hbase[V](table, columns)`, where types of row key and qualifier are String.
These alternatives apply for all `sc.hbase()` and `sc.hbaseTS()` methods.

A second possibility is to get the whole column families. This can be useful if you do not know in advance which will be the column names. You can do this with the method `sc.hbase[A]`, like

    val table = "t1"
    val families = Set("cf1", "cf2")
    val rdd = sc.hbase[String](table, families)

The output, like `sc.hbase[A]`, is a `RDD[(String, Map[String, Map[String, A]])]`.

If you need to read also timestamps, you can use in both cases `sc.hbaseTS[K, Q, V]` and obtain a `RDD[(K, Map[String, Map[Q, (V, Long)]])]`. Each element of the resulting RDD is a key/value pair, where the key is the rowkey from HBase and the value is a nested map which associates column family and column to the tuple (value, timestamp).

Finally, there is a lower level access to the raw `org.apache.hadoop.hbase.client.Result` instances. For this, just do

    val table = "t1"
    val rdd = sc.hbase[K](table)

The return value of `sc.hbase` is a `RDD[(K, Result)]`. The first element is the rowkey, while the second one is an instance of `org.apache.hadoop.hbase.client.Result`, so you can use the raw HBase API to query it.

HBase side filters are also supported by providing a custom Filter or Scan object:

    val filter = new PrefixFilter(Bytes.toBytes("abc"))
    val table = "t1"
    val families = Set("cf1", "cf2")
    val rdd = sc.hbase[String](table, families, filter)

### Writing to HBase

In order to write to HBase, some methods are added on certain types of RDD.

The first one is parallel to the way you read from HBase. Assume you have an `RDD[(K, Map[String, Map[Q, V]])]` and there is `Writes[K]`, `Writes[Q]`, and `Writes[V]` in scope. Then you can write to HBase with the method `toHBase`, like

    val table = "t1"
    val rdd: RDD[(K, Map[String, Map[Q, V]])] = ...
    rdd.toHBase(table)

A simplified form is available in the case that one only needs to write on a single column family. Then a similar method is available on `RDD[(K, Map[Q, V])]`, which can be used as follows

    val table = "t1"
    val cf = "cf1"
    val rdd: RDD[(K, Map[Q, V])] = ...
    rdd.toHBase(table, cf)

or, if you have a fixed set of columns, like

    val table = "t1"
    val cf = "cf1"
    val headers: Seq[Q] = ...
    val rdd: RDD[(K, Seq[V])] = ...
    rdd.toHBase(table, cf, headers)

where `headers` are column names for `Seq[V]` values.

If you need to write timestamps, you can use a tuple (V, Long) in your RDD, where the second element represents the timestamp, like

    val rdd: RDD[(K, Map[String, Map[Q, (V, Long)]])] = ...

or, for the simplified form, like

    val rdd: RDD[(K, Map[Q, (V, Long)])] = ...

or, with a fixed set of columns

    val rdd: RDD[(K, Seq[(V, Long)])] = ...

You can have a look at `WriteTsvToHBase.scala` in [hbase-rdd-examples project](https://github.com/unicredit/hbase-rdd-examples) on how to write a TSV file from `Hdfs` to `HBase`

### Deleting from HBase

In order to delete from HBase, some methods are added on certain types of RDD.

Assume you have an `RDD[(K, Map[String, Set[Q])]` of row keys and a map of families / set of columns. Then you can delete from HBase with the method `deleteHBase`, like

    val table = "t1"
    val rdd: RDD[(K, Map[String, Set[Q])] = ...
    rdd.deleteHBase(table)

A simplified form is available in the case that one only needs to delete from a single column family. Then a similar method is available on `RDD[(K, Set[Q])]` of row keys and a set of columns, which can be used as follows

    val table = "t1"
    val cf = "cf1"
    val rdd: RDD[(K, Set[Q])] = ...
    rdd.deleteHBase(table, cf)

or, if you want to delete a fixed set of columns of one column family, or whole column families, or whole rows, some methods are available on `RDD[K]` of row keys, which can be used as follows

    val table = "t1"
    val cf = "cf1"
    val headers: Set[Q] = ...
    val rdd: RDD[K] = ...
    rdd.deleteHBase(table, cf, headers)

or

    val cfs = Set("cf1", "cf2")
    rdd.deleteHBase(table, cfs)

or

    rdd.deleteHBase(table)

If you need to delete with timestamps, you can use a tuple (String, Long) in your RDD, where the first element is a column and the second element represents the timestamp, like

    val rdd: RDD[(K, Map[String, Set[(Q, Long)]])] = ...

or, for the simplified form, like

    val rdd: RDD[(K, Set[(Q, Long)])] = ...

### Bulk load to HBase, using HFiles

In case of massive writing to HBase, writing Put objects directly into the table can be inefficient and can cause HBase to be unresponsive (e.g. it can trigger region splitting).
A better approach is to create HFiles instead, and than call LoadIncrementalHFiles job to move them to HBase's file system. Unfortunately this approach is quite cumbersome, as it implies the following steps:

1. Make sure the table exists and has region splits so that rows are evenly distributed into regions (for better performance).

2. Implement and execute a map (and reduce) job to write ordered Put or KeyValue objects to HFile files, using HFileOutputFormat2 output format. The reduce phase is configured behind the scenes with a call to HFileOutputFormat2.configureIncrementalLoad.

3. Execute LoadIncrementalHFiles job to move HFile files to HBase's file system.

4. Cleanup temporary files and folders

Now you can perform steps 2 to 4 with a call to `toHBaseBulk`, like

    val table = "t1"
    val rdd: RDD[(K, Map[String, Map[Q, V]])] = ...
    rdd.toHBaseBulk(table)

A simplified form is available in the case that one only needs to write on a single column family

    val table = "t1"
    val cf = "cf1"
    val rdd: RDD[(K, Map[Q, V])] = ...
    rdd.toHBaseBulk(table, cf)

or, if you have a fixed set of columns, like

    val table = "t1"
    val cf = "cf1"
    val headers: Seq[Q] = ...
    val rdd: RDD[(K, Seq[V])] = ...
    rdd.toHBaseBulk(table, cf, headers)

where `headers` are column names for `Seq[A]` values.

If you need to write timestamps, you can use a tuple `(A, Long)` in your `RDD`, where the second element represents the timestamp, like

    val rdd: RDD[(K, Map[String, Map[Q, (V, Long)]])] = ...

or, for the simplified form, like

    val rdd: RDD[(K, Map[Q, (V, Long)])] = ...

or, in case of a fixed set of columns, like

    val rdd: RDD[(K, Seq[(V, Long)])] = ...

But what about step 1? For this, an `Admin` object with a few helper methods come to the rescue.
You must open a connection to HBase (as required since version 1.0.0), by instancing it

      val admin = Admin()

and then

- `admin.tableExists(tableName: String, family: String)`: checks if the table exists, and returns true or false accordingly. If the table `tableName` exists but the column family `family` does not, an `IllegalArgumentException` is thrown
- `admin.tableExists(tableName: String, families: Set[String])`: checks if the table exists, and returns true or false accordingly. If the table `tableName` exists but at least one of `families` does not, an `IllegalArgumentException` is thrown
- `admin.snapshot(tableName: String)`: creates a snapshot of table `tableName`, named `<tablename>_yyyyMMddHHmmss` (suffix is the date and time of the snapshot operation)
- `admin.snapshot(tableName: String, snapshotName: String)`: creates a snapshot of table `tableName`, named `snapshotName
- `admin.createTable(tableName: String, family: String, splitKeys: Seq[K])`: creates a table `tableName` with column family `family` and regions defined by a sorted sequence of split keys `splitKeys`
- `admin.createTable(tableName: String, families: Set[String], splitKeys: Seq[K])`: creates a table `tableName` with column families `families` and regions defined by a sorted sequence of split keys `splitKeys`
- `admin.createTable(tableName: String, families: Set[String])`: creates a table `tableName` with column families `families`
- `admin.createTable(tableName: String, families: String*)`: creates a table `tableName` with column families `families`
- `admin.disableTable(tableName: String)`: disables table `tableName` (a table must be disabled before deletion)
- `admin.deleteTable(tableName: String)`: deletes table `tableName`
- `admin.truncateTable(tableName: String, preserveSplits: Boolean)`: truncates table `tableName`, optionally preserving region splits
- `admin.computeSplits(rdd: RDD[K], regionsCount: Int)`: given an `RDD`of keys and desired number of regions (`regionsCount`), returns a sorted sequence of split keys, to be used with `createTable()`

finally you must close the connection to HBase with

    admin.close

You can have a look at `ImportTsvToHFiles.scala` in [hbase-rdd-examples project](https://github.com/unicredit/hbase-rdd-examples) on how to bulk load a TSV file from `Hdfs` to `HBase`

#### Set the Number of HFiles per Region per Family

For best performance, HBase should use 1 HFile per region per family. On the other hand, the more HFiles you use, the more partitions you have in your Spark job, hence Spark tasks run faster and consume less memory heap.
You can fine tune this opposite requirement by passing an additional optional parameter to `toHBaseBulk()` method, `numFilesPerRegionPerFamily=<N>` where N (default is 1) is a number between 1 and `hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily` parameter (default is 32), e.g.

    rdd.toHBaseBulk(table, numFilesPerRegionPerFamily=32)

or

    rdd.toHBaseBulk(table, cf, numFilesPerRegionPerFamily=32)

or

    rdd.toHBaseBulk(table, cf, headers, numFilesPerRegionPerFamily=32)
