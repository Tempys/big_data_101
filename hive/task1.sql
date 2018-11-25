CREATE TABLE test ( id BIGINT,  cnt BIGINT, is_booking TINYINT,hotel_market INT, hotel_countr INT, hotel_continent INT, srch_destination_type_id INT, srch_destination_id INT, srch_rm_cnt INT, srch_children_cnt INT, srch_adults_cnt INT, srch_co STRING, srch_ci STRING, channel INT, is_package INT, is_mobile TINYINT, user_id INT, orig_destination_distance DOUBLE, user_location_city INT, user_location_region INT, user_location_country INT, posa_continent INT, site_name INT, date_time STRING )
 ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='hdfs:///schemas/test.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';






LOAD DATA INPATH '/testavro/test.avro' OVERWRITE INTO TABLE test;

CREATE TABLE train ( id BIGINT,  cnt BIGINT, is_booking TINYINT,hotel_market INT, hotel_countr INT, hotel_continent INT, srch_destination_type_id INT, srch_destination_id INT, srch_rm_cnt INT, srch_children_cnt INT, srch_adults_cnt INT, srch_co STRING, srch_ci STRING, channel INT, is_package INT, is_mobile TINYINT, user_id INT, orig_destination_distance DOUBLE, user_location_city INT, user_location_region INT, user_location_country INT, posa_continent INT, site_name INT, date_time STRING )
 ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='hdfs:///schemas/test.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';






LOAD DATA INPATH '/trainavro/trains.avro' OVERWRITE INTO TABLE train;





CREATE TABLE destination ( srch_destination_id INT,	d1 DOUBLE,	d2 DOUBLE,	d3 DOUBLE,	d4 DOUBLE,	d5 DOUBLE,	d6 DOUBLE,	d7 DOUBLE,	d8 DOUBLE,	d9 DOUBLE,	d10 DOUBLE,	d11 DOUBLE,	d12 DOUBLE,	d13 DOUBLE,	d14 DOUBLE,	d15 DOUBLE,	d16 DOUBLE,	d17 DOUBLE,	d18 DOUBLE,	d19 DOUBLE,	d20 DOUBLE,	d21 DOUBLE,	d22 DOUBLE,	d23 DOUBLE,	d24 DOUBLE,	d25 DOUBLE,	d26 DOUBLE,	d27 DOUBLE,	d28 DOUBLE,	d29 DOUBLE,	d30 DOUBLE,	d31 DOUBLE,	d32 DOUBLE,	d33 DOUBLE,	d34 DOUBLE,	d35 DOUBLE,	d36 DOUBLE,	d37 DOUBLE,	d38 DOUBLE,	d39 DOUBLE,	d40 DOUBLE,	d41 DOUBLE,	d42 DOUBLE,	d43 DOUBLE,	d44 DOUBLE,	d45 DOUBLE,	d46 DOUBLE,	d47 DOUBLE,	d48 DOUBLE,	d49 DOUBLE,	d50 DOUBLE,	d51 DOUBLE,	d52 DOUBLE,	d53 DOUBLE,	d54 DOUBLE,	d55 DOUBLE,	d56 DOUBLE,	d57 DOUBLE,	d58 DOUBLE,	d59 DOUBLE,	d60 DOUBLE,	d61 DOUBLE,	d62 DOUBLE,	d63 DOUBLE,	d64 DOUBLE,	d65 DOUBLE,	d66 DOUBLE,	d67 DOUBLE,	d68 DOUBLE,	d69 DOUBLE,	d70 DOUBLE,	d71 DOUBLE,	d72 DOUBLE,	d73 DOUBLE,	d74 DOUBLE,	d75 DOUBLE,	d76 DOUBLE,	d77 DOUBLE,	d78 DOUBLE,	d79 DOUBLE,	d80 DOUBLE,	d81 DOUBLE,	d82 DOUBLE,	d83 DOUBLE,	d84 DOUBLE,	d85 DOUBLE,	d86 DOUBLE,	d87 DOUBLE,	d88 DOUBLE,	d89 DOUBLE,	d90 DOUBLE,	d91 DOUBLE,	d92 DOUBLE,	d93 DOUBLE,	d94 DOUBLE,	d95 DOUBLE,	d96 DOUBLE,	d97 DOUBLE,	d98 DOUBLE,	d99 DOUBLE,	d100 DOUBLE,	d101 DOUBLE,	d102 DOUBLE,	d103 DOUBLE,	d104 DOUBLE,	d105 DOUBLE,	d106 DOUBLE,	d107 DOUBLE,	d108 DOUBLE,	d109 DOUBLE,	d110 DOUBLE,	d111 DOUBLE,	d112 DOUBLE,	d113 DOUBLE,	d114 DOUBLE,	d115 DOUBLE,	d116 DOUBLE,	d117 DOUBLE,	d118 DOUBLE,	d119 DOUBLE,	d120 DOUBLE,	d121 DOUBLE,	d122 DOUBLE,	d123 DOUBLE,	d124 DOUBLE,	d125 DOUBLE,	d126 DOUBLE,	d127 DOUBLE,	d128 DOUBLE,	d129 DOUBLE,	d130 DOUBLE,	d131 DOUBLE,	d132 DOUBLE,	d133 DOUBLE,	d134 DOUBLE,	d135 DOUBLE,	d136 DOUBLE,	d137 DOUBLE,	d138 DOUBLE,	d139 DOUBLE,	d140 DOUBLE,	d141 DOUBLE,	d142 DOUBLE,	d143 DOUBLE,	d144 DOUBLE,	d145 DOUBLE,	d146 DOUBLE,	d147 DOUBLE,	d148 DOUBLE,	d149 DOUBLE )
 ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  WITH SERDEPROPERTIES (
    'avro.schema.url'='hdfs:///schemas/destinations.avsc')
  STORED as INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';


LOAD DATA INPATH '/destinationavro/destinations.avro' OVERWRITE INTO TABLE destination;