name := "simpleProfiler"

version := "1.0"

scalaVersion := "2.11.5"


val sparkVersion = "1.6.0"

libraryDependencies ++= Seq (
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-yarn_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-hive_2.11" % sparkVersion % "provided",
  "org.spark-project.hive" % "hive-metastore" % "1.2.1.spark",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0"
)
