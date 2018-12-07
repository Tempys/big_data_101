package com.dubovskyi.streaming


import org.apache.avro.generic.GenericData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._











object Task2 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
    import org.apache.avro.SchemaBuilder
    import Helper.schema

    var conf = new SparkConf()
        conf.set("spark.io.compression.codec", "snappy")


    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("subscribe", "streaming2")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("data"))
      .select("data.*")
      //.show(100)
     .write
     .format("com.databricks.spark.csv")
     .option("header", "true")
     .save("hdfs://sandbox-hdp.hortonworks.com:8020/result/result.csv")





  }

}
