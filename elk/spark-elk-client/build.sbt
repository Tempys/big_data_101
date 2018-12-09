name := "spark-elk-client"

version := "0.1"

scalaVersion :=  "2.11.12"

libraryDependencies += "junit" % "junit" % "4.10" % Test

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
/*  "com.databricks" %% "spark-avro" % "3.2.0",*/
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1",
  "org.elasticsearch" % "elasticsearch-hadoop" % "6.5.1"

)