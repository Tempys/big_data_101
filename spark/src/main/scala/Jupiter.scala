import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Jupiter {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._


/*
    var conf = new SparkConf()




      spark.read
             .format("com.databricks.spark.avro")
             .load("hdfs://sandbox-hdp.hortonworks.com:8020/trainavro/trains.avro")
             .filter(col("srch_adults_cnt").equalTo(2))
             .groupBy("hotel_continent","hotel_country","hotel_market")
             .agg(count("user_id").as("count"))
             .orderBy(desc("count"))
             .show(3)*/


}

}
