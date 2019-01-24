import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SortingSecondKey {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.functions._
    import java.sql.Timestamp
    import org.apache.spark.sql.expressions.Window._

    final case class Data(name: String, datetime: Timestamp, status: String)

    var conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

    val  last = "";

    val data =  spark.createDataFrame(Seq(
      ("object1", Timestamp.valueOf("2016-05-21 05:20:56")),
      ("object2", Timestamp.valueOf("2016-05-21 05:20:57")),
      ("object3", Timestamp.valueOf("2016-05-21 05:20:58")),
      ("object2", Timestamp.valueOf("2016-05-21 05:20:58")),
      ("object3", Timestamp.valueOf("2016-05-21 05:20:59")),
      ("object1", Timestamp.valueOf("2016-05-21 05:21:00"))
    )).toDF("id","date")

    val byIdWithDateDesc = Window.partitionBy('id).orderBy('date)

    data
      .filter(col("date").>(last))
        .withColumn("t", lag('date, 1).over(byIdWithDateDesc))
        .withColumn("duration", unix_timestamp($"date") - unix_timestamp($"t"))
      .filter(col("duration").>(3))
        .show()
  }
}
