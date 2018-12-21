package com.dubovskyi.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Main class which start spark and start streaming from kafka to hdfs
  */
object Streaming {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import org.apache.avro.SchemaBuilder
    import Helper.schema

      val bootstrapServers = "localhost:9092"
      val topic = "streaming3"
      val hdfsUrl = "hdfs://sandbox-hdp.hortonworks.com:8020/result4/result4.csv"
    /**
      * this data need for calculates metrics and for comparing the perfomance with Batch approach
      */
      val metricsConfig = "C:\\projects\\cources\\big_data_101\\streaming\\spark-client\\src\\main\\resources\\metric.properties"

    var conf = new SparkConf()
    conf.set("spark.io.compression.codec", "snappy")
    conf.set("spark.sql.streaming.checkpointLocation", "/save12/location-streaming")
    conf.set("spark.metrics.conf",metricsConfig)

    val spark = SparkSession.builder()
      .config(conf)
      .master("local")
      .getOrCreate()
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers )
      .option("subscribe", topic )
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("data"))
      .select("data.*")
      .writeStream
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .start(hdfsUrl)
      .awaitTermination()

  }
}
