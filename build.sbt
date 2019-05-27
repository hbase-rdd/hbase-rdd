lazy val common = Seq(
  name := "hbase-rdd",
  organization := "eu.unicredit",
  crossScalaVersions := Seq("2.11.12", "2.12.8"),
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-language:postfixOps",
    "-language:implicitConversions",
    "-language:reflectiveCalls"
  ),
  resolvers += Resolver.sonatypeRepo("releases"),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  ),
  fork in Test := true
) ++ sonatypePublish

def sparkDependencies(version: String) = Seq(
  "org.apache.spark" %% "spark-core" % version % "provided"
)

def hbaseDependencies(version: String) = Seq(
  "org.apache.hbase" % "hbase-common" % version % "provided",
  "org.apache.hbase" % "hbase-mapreduce" % version % "provided",
  "org.apache.hbase" % "hbase-server" % version % "provided",
  "org.apache.hbase" % "hbase-common" % version % "test" classifier "tests",
  "org.apache.hbase" % "hbase-server" % version % "test" classifier "tests",
  "org.apache.hbase" % "hbase-http" % version % "test",
  "org.apache.hbase" % "hbase-metrics" % version % "test",
  "org.apache.hbase" % "hbase-metrics-api" % version % "test",
  "org.apache.hbase" % "hbase-zookeeper" % version % "test",
  "org.apache.hbase" % "hbase-zookeeper" % version % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop-compat" % version % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % version % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % version % "test",
  "org.apache.hbase" % "hbase-hadoop2-compat" % version % "test" classifier "tests"
)

def hadoopDependencies(version: String) = Seq(
  "org.apache.hadoop" % "hadoop-common" % version % "test",
  "org.apache.hadoop" % "hadoop-common" % version % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % version % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % version % "test" classifier "tests"
)

def dependencyOverridesForSpark() = Seq( // required by Spark 2.4
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7" % "test",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7" % "test",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7" % "test"
)

lazy val cdh = project
  .in(file("cdh"))
  .settings(common)
  .settings(
    version := (version in root).value + "-cdh62",
    crossScalaVersions := Seq("2.11.12"),
    sourceDirectory := (sourceDirectory in root).value,
    resolvers ++= Seq(
      "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
    ),
    libraryDependencies ++=
      sparkDependencies("2.4.0-cdh6.2.0") ++
        hbaseDependencies("2.1.0-cdh6.2.0") ++
        hadoopDependencies("3.0.0-cdh6.2.0")
  )

lazy val hdp = project
  .in(file("hdp"))
  .settings(common)
  .settings(
    version := (version in root).value + "-hdp31",
    crossScalaVersions := Seq("2.11.12"),
    sourceDirectory := (sourceDirectory in root).value,
    libraryDependencies ++=
      sparkDependencies("2.3.2") ++
        hbaseDependencies("2.0.2") ++
        hadoopDependencies("2.9.2"),
    dependencyOverrides ++= dependencyOverridesForSpark()
  )

lazy val root = project
  .in(file("."))
  .settings(common)
  .settings(
    version := "0.9.0",
    libraryDependencies ++=
      sparkDependencies("2.4.3") ++
        hbaseDependencies("2.1.4") ++
        hadoopDependencies("2.9.2"),
    dependencyOverrides ++= dependencyOverridesForSpark()
  )

lazy val sonatypePublish = Seq(
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  publishTo := Some(sonatypeDefaultResolver.value),
  credentials += Credentials(Path.userHome / ".ivy2" / "sonatype.credentials"),
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
)
