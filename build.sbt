name := "hbase-rdd"

organization := "eu.unicredit"

version := "0.9.0-SNAPSHOT"

crossScalaVersions := Seq("2.11.12", "2.12.8")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:reflectiveCalls"
)

val sparkVersion = "2.4.3"
val hbaseVersion = "1.4.9"
val hadoopVersion = "2.6.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
  // for tests
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-metrics" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-metrics-api" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.hbase" % "hbase-hadoop2-compat" % hbaseVersion % "test" classifier "tests" excludeAll ExclusionRule(organization = "org.apache.hadoop"),
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test",
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "test" classifier "tests"
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
