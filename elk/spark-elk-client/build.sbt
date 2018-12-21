name := "spark-elk-client"

version := "0.1"

scalaVersion :=  "2.11.12"

resolvers += "jitpack" at "https://jitpack.io"

/*libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v0.16.0" % "test"*/
// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test



libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1",
  "org.elasticsearch" % "elasticsearch-hadoop" % "6.5.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1"  % "test" classifier "tests",
"org.apache.spark" %% "spark-core" % "2.3.1"  % "test" classifier "tests",
   "org.apache.spark" %% "spark-catalyst" % "2.3.1"  % "test" classifier "tests"

)



