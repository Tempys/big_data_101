package com.dubovskyi.streaming


import org.apache.avro.generic.GenericData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._


/**
  * Main class which start spark and start batching from kafka to hdfs
  */
object Batching {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import org.apache.avro.SchemaBuilder
    import Helper.schema

    var conf = new SparkConf()
        conf.set("spark.io.compression.codec", "snappy")

    val bootstrapServers = "localhost:9092"
    val topic = "streaming5"
    val hdfsUrl = "hdfs://sandbox-hdp.hortonworks.com:8020/result8/result8.csv"

      val spark = SparkSession.builder()
        .config(conf)
        .master("local").getOrCreate()
        .read
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", topic)
       // .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"),schema).as("data"))
        .select("data.*")
        //.show(10)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(hdfsUrl)





  }

}
