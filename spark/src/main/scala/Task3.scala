import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Task3 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._



    var conf = new SparkConf()

    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()

    spark.read
      .format("com.databricks.spark.avro")
      .load("hdfs://sandbox-hdp.hortonworks.com:8020/trainavro/trains.avro")
      .filter(col("is_booking").equalTo(0).and(col("srch_children_cnt").>(0)))
      .groupBy("hotel_continent","hotel_country","hotel_market")
      .agg(count("user_id").as("count"))
      .orderBy(desc("count"))
      .show(3)
  }

}
