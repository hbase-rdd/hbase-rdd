import SonatypeKeys._

sonatypeSettings

name := "hbase-rdd"

organization := "eu.unicredit"

version := "0.4.0-SNAPSHOT"

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
  "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.2" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.98.1-cdh5.1.3" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.98.1-cdh5.1.3" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.98.1-cdh5.1.3" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided"
)

publishMavenStyle := true

pomIncludeRepository := { x => false }

credentials += Credentials(Path.userHome / ".ivy2" / "sonatype.credentials")