package com.dubovskyi.streaming

import org.apache.spark.sql.types._

object Helper {

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

}
