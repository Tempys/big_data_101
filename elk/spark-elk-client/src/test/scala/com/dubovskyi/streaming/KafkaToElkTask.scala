package com.dubovskyi.streaming


import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.plans.PlanTest


class KafkaToElkTask extends QueryTest with SharedSQLContext  {

  import testImplicits._

  test("function get_json_object") {
    val df: DataFrame = Seq(("""{"channel": 55, "hotel_cluster": 66, "is_booking": 0  }""", "")).toDF()

    val schema = new StructType()
      .add("hotel_cluster", IntegerType)
      .add("is_booking", IntegerType)
      .add("channel", IntegerType)

    checkAnswer(
      df.select(from_json(col("value"),schema)),
      Row(Row(55,66,0)) :: Nil )
  }

}
