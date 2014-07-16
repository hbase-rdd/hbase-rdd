name := "hbase-rdd"

organization := "unicredit"

version := "0.2.1"

scalaVersion := "2.10.3"

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
  "org.apache.spark" %% "spark-core" % "0.9.1" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.1" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.1" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.96.1.1-cdh5.0.1" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.2.9" % "provided"
)
