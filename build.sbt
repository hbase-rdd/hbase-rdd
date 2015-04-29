import SonatypeKeys._

sonatypeSettings

name := "hbase-rdd"

organization := "eu.unicredit"

version := "0.4.99-writes"

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

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.98.6-cdh5.3.1" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.98.6-cdh5.3.1" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.98.6-cdh5.3.1" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided"
)

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
