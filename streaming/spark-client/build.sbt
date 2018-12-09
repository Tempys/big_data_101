name := "spark-client"

version := "0.1"

scalaVersion :=  "2.11.12"

/*resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io",
  Resolver.mavenLocal
)*/
Compile/mainClass := Some("com.dubovskyi.streaming.Streaming")


libraryDependencies += "junit" % "junit" % "4.10" % Test

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "com.databricks" %% "spark-avro" % "3.2.0",
   "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"
  /*  "com.databricks" %% "spark-avro" % "3.2.0",*/
 // "io.confluent" % "kafka-avro-serializer" % "4.1.1",

  // "com.fasterxml.jackson.core" % "jackson-core" % "2.9.0",
   // "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.0",
   //"com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.0",

   //"za.co.absa" %% "abris" % "2.0.0" 


)