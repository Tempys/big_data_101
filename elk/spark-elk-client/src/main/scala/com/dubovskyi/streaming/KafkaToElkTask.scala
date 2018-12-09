package com.dubovskyi.streaming




import org.apache.avro.generic.GenericData
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._



object KafkaToElkTask {

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
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("subscribe", "streaming3")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("data"))
      .select("data.*")



    .writeStream
      .option("checkpointLocation", "/save/location")
      .format("org.elasticsearch.spark.sql")
      .start("streaming/default")
      .awaitTermination()





  }

}
