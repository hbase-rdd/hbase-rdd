org.scalastyle.sbt.ScalastylePlugin.Settings

sonatypeSettings

lazy val common = Seq(
  name := "hbase-rdd",
  organization := "eu.unicredit",
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-language:postfixOps",
    "-language:implicitConversions",
    "-language:reflectiveCalls"
  ),
  resolvers += Resolver.sonatypeRepo("releases"),
  libraryDependencies ++= Seq(
    "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  ),
  fork in Test := true
) ++ sonatypePublish

def sparkDependencies(version: String) = Seq(
  "org.apache.spark" %% "spark-core" % version % "provided"
)

def hbaseDependencies(version: String) = Seq(
  "org.apache.hbase" % "hbase-common" % version % "provided",
  "org.apache.hbase" % "hbase-client" % version % "provided",
  "org.apache.hbase" % "hbase-server" % version % "provided",
  "org.apache.hbase" % "hbase-common" % version % "test" classifier "tests",
  "org.apache.hbase" % "hbase-server" % version % "test" classifier "tests" exclude("org.mortbay.jetty", "servlet-api-2.5"),
  "org.apache.hbase" % "hbase-hadoop-compat" % version % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % version % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % version % "test" exclude("javax.servlet", "servlet-api"),
  "org.apache.hbase" % "hbase-hadoop2-compat" % version % "test" classifier "tests"
)

def hadoopDependencies(version: String) = Seq(
  "org.apache.hadoop" % "hadoop-common" % version % "test" exclude("javax.servlet", "servlet-api"),
  "org.apache.hadoop" % "hadoop-common" % version % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-hdfs" % version % "test" exclude("javax.servlet", "servlet-api"),
  "org.apache.hadoop" % "hadoop-hdfs" % version % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % version % "test" classifier "tests" exclude("javax.servlet", "servlet-api")
)

lazy val cdh = project
  .in(file("cdh"))
  .settings(common)
  .settings(
    version := (version in root).value + "-cdh55",
    sourceDirectory := (sourceDirectory in root).value,
    resolvers ++= Seq(
      "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
    ),
    libraryDependencies ++=
      sparkDependencies("1.5.0") ++
        hbaseDependencies("1.0.0-cdh5.5.6") ++
        hadoopDependencies("2.6.0-cdh5.5.6")
  )

lazy val mapr = project
  .in(file("mapr"))
  .settings(common)
  .settings(
    version := (version in root).value + "-mapr1602",
    sourceDirectory := (sourceDirectory in root).value,
    resolvers ++= Seq(
      "MapR repo" at "http://repository.mapr.com/maven"
    ),
    libraryDependencies ++=
      sparkDependencies("1.5.2-mapr-1602") ++
        hbaseDependencies("1.1.1-mapr-1602") ++
        hadoopDependencies("2.7.0-mapr-1602")
  )

lazy val root = project
  .in(file("."))
  .settings(common)
  .settings(
    version := "0.8.0",
    libraryDependencies ++=
      sparkDependencies("1.5.0") ++
        hbaseDependencies("1.0.0") ++
        hadoopDependencies("2.6.0")
  )

lazy val sonatypePublish = sonatypeSettings ++ Seq(
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
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
