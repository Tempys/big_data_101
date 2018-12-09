package com.dubovskyi.streaming

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HdfsToElkTask {

  /**
    * Main class : stream data from Hdfs to Elasticseacrch
    */
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import org.apache.avro.SchemaBuilder
    import Helper.schema
    import org.elasticsearch.spark.sql._

    var conf = new SparkConf()
    conf.set("spark.io.compression.codec", "snappy")
    //   .set("es.index.auto.create", "true")
    conf.set("es.nodes","localhost")
    conf.set("es.port" , "9200")
    conf.set("es.nodes.wan.only", "true")


    val spark = SparkSession.builder()

    LogManager.getRootLogger.setLevel(Level.DEBUG)

    spark.config(conf)
      .master("local")
      .getOrCreate()

      .readStream
      //.format("kafka")
     // .option("sep", ",")
      .schema(schema = schema)      // Specify schema of the csv files
      .csv("hdfs://sandbox-hdp.hortonworks.com:8020/result4/result4.csv")
      .writeStream
      .option("checkpointLocation", "/save4/location")
      .format("org.elasticsearch.spark.sql")
      .start("streaming/default")
      .awaitTermination()



  }
}
