import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
  * Find the most popular country where hotels are booked and searched from the same country
  * Get data from hdfs in avro format
  */
object Task2 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._



    var conf = new SparkConf()

    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()

    spark.read
      .format("com.databricks.spark.avro")
      .load("hdfs://sandbox-hdp.hortonworks.com:8020/trainavro/trains.avro")
      .filter(col("is_booking").equalTo(1).and(col("hotel_country").equalTo(col("user_location_country"))))
      .groupBy("hotel_country")
      .agg(count("user_id").as("count"))
      .orderBy(desc("count"))
      .show(3)
  }

}
