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

    var conf = new SparkConf()
    conf.set("spark.io.compression.codec", "snappy")
    conf.set("spark.sql.streaming.checkpointLocation", "/save12/location-streaming")
    conf.set("spark.metrics.conf","C:\\projects\\cources\\big_data_101\\streaming\\spark-client\\src\\main\\resources\\metric.properties")

    val spark = SparkSession.builder()
      .config(conf)
      .master("local")
      .getOrCreate()
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "streaming3")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("data"))
      .select("data.*")
      .writeStream
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .start("hdfs://sandbox-hdp.hortonworks.com:8020/result4/result4.csv")
      .awaitTermination()

  }
}
