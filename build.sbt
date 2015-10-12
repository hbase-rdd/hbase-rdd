import SonatypeKeys._

sonatypeSettings

name := "hbase-rdd"

organization := "eu.unicredit"

version := "0.6.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:reflectiveCalls"
)

org.scalastyle.sbt.ScalastylePlugin.Settings

resolvers ++= Seq(
  "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
)

val sparkVersion = "1.2.0"
val hbaseVersion = "0.98.6-cdh5.3.1"
val hadoopVersion = "2.5.0-cdh5.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
  // for tests
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "test" classifier "tests" exclude("org.mortbay.jetty", "servlet-api-2.5"),
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" exclude("javax.servlet", "servlet-api") exclude("com.google.guava", "guava"),
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" exclude("javax.servlet", "servlet-api"),
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion % "test" classifier "tests" exclude("javax.servlet", "servlet-api")
    excludeAll ExclusionRule(organization = "org.codehaus.jackson")
)

fork in Test := true

publishMavenStyle := true

pomIncludeRepository := { x => false }

credentials += Credentials(Path.userHome / ".ivy2" / "sonatype.credentials")

pomExtra := {
  <url>https://github.com/unicredit/hbase-rdd</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:github.com/unicredit/hbase-rdd</connection>
    <developerConnection>scm:git:git@github.com:unicredit/hbase-rdd</developerConnection>
    <url>github.com/unicredit/hbase-rdd</url>
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
