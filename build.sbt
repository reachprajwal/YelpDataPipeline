name := "yelpETL"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.1.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.1",
  "org.apache.spark" % "spark-mllib_2.11" % "2.1.1"
)