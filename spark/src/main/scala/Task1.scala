import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object Task1 {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._

    var conf = new SparkConf()
        conf.set("fs.default.name", "hdfs://quickstart.cloudera:8020")
    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()


             spark.read
                   .format("com.databricks.spark.avro")
                   .load("hdfs://quickstart.cloudera:8020/trainavro/trains.avro")
                   .filter(col("srch_adults_cnt").equalTo(2))
                   .groupBy("hotel_continent","hotel_country","hotel_market")
                   .agg(count("user_id").as("count"))
                   .orderBy(desc("count"))
                   .show(3)
  }

}
