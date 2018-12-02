package com.dubovskyi.streaming


import org.apache.avro.generic.GenericData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}











object Task2 {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.functions._
    import org.apache.avro.SchemaBuilder

    val schema = new StructType()
                          .add("a", StringType)
                           .add("b", StringType)

    var conf = new SparkConf()
        conf.set("spark.io.compression.codec", "snappy")


    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("subscribe", "streaming1")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"),schema).as("data"))
      .select("data.*")
      //.show()
     // .select("a","b")
     // .show()
      //.toDF()
      .write
      .format("com.databricks.spark.csv")
       .option("header", "true")
       .save("C:\\myfile.csv")


      // .show()

   //  .fromAvro("value","src\\main\\resources\\event.avsc")(RETAIN_SELECTED_COLUMN_ONLY )
   //   .writeStream.format("console").start().awaitTermination()
      //.load()

      /*.select(from_avro($"key", SchemaBuilder.builder().stringType()).as("key"),
              from_avro($"value", SchemaBuilder.builder().intType()).as("value"))*/
     // .show()


  }

}
