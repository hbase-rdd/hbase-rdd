name := "hbase-rdd"

organization := "eu.unicredit"

version := "0.9.1"

crossScalaVersions := Seq("2.11.12", "2.12.13")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:reflectiveCalls"
)

resolvers ++= Seq(
  "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
)

val sparkVersion = "2.4.7"
val hbaseVersion = "2.1.0-cdh6.3.4"
val hadoopVersion = "3.0.0-cdh6.3.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-mapreduce" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
  // for tests
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-http" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-metrics" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-metrics-api" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-zookeeper" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-zookeeper" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests"
)

// required by Spark 2.4
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7" % "test",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7" % "test",
)

fork in Test := true

publishMavenStyle := true

pomIncludeRepository := { _ => false }

publishTo := sonatypePublishToBundle.value

credentials += Credentials(Path.userHome / ".ivy2" / "sonatype.credentials")

pomExtra := {
  <url>https://github.com/hbase-rdd/hbase-rdd</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:github.com/hbase-rdd/hbase-rdd</connection>
    <developerConnection>scm:git:git@github.com:hbase-rdd/hbase-rdd</developerConnection>
    <url>github.com/hbase-rdd/hbase-rdd</url>
  </scm>
  <developers>
    <developer>
      <id>andreaferretti</id>
      <name>Andrea Ferretti</name>
      <url>https://github.com/andreaferretti/</url>
    </developer>
    <developer>
      <id>fralken</id>
      <name>Francesco Montecuccoli Degli Erri</name>
      <url>https://github.com/fralken/</url>
    </developer>
  </developers>
}
