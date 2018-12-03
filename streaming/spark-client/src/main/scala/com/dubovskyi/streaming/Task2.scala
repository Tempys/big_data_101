package com.dubovskyi.streaming


import org.apache.avro.generic.GenericData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._











object Task2 {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.functions._
    import org.apache.avro.SchemaBuilder

    val schema = new StructType()
      .add("hotel_cluster", IntegerType)
      .add("cnt", LongType)
      .add("is_booking", IntegerType)
      .add("hotel_market", IntegerType)
      .add("hotel_country", IntegerType)
      .add("hotel_continent", IntegerType)
      .add("srch_destination_type_id", IntegerType)
      .add("srch_rm_cnt", IntegerType)
      .add("srch_children_cnt", IntegerType)
      .add("srch_adults_cnt", IntegerType)
      .add("srch_co", StringType)
      .add("srch_ci", StringType)
      .add("channel", IntegerType)
      .add("is_package", IntegerType)
      .add("is_mobile", IntegerType)
      .add("user_id", IntegerType)
      .add("orig_destination_distance", DoubleType)
      .add("user_location_city", IntegerType)
      .add("user_location_region", IntegerType)
      .add("user_location_country", IntegerType)
      .add("posa_continent", IntegerType)
      .add("site_name", IntegerType)
      .add("date_time", StringType)


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
     // .show()

      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://sandbox-hdp.hortonworks.com:8020/result/result.csv")





  }

}
