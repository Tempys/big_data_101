import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.internal.util.TableDef.Column

object Task1 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._



    var conf = new SparkConf()

    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()


              spark.read
                   .format("com.databricks.spark.avro")
                   .load("hdfs://sandbox-hdp.hortonworks.com:8020/trainavro/trains.avro")
                   .filter(col("srch_adults_cnt").equalTo(2))
                   .groupBy("hotel_continent","hotel_country","hotel_market")
                   .agg(count("user_id").as("count"))
                   .orderBy(desc("count"))
                   .show(3)
  }

}
