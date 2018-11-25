Project:

Upload data from hdfs in csv file and convert into avro and save in hdfs

Configuration:

 from: list initial path  scv files in hdfs with delimiter ","
 to: list on destinations path where we want to save file in hdfs with delimiter ","
 avro.schema.path: list of avro schemas with delimiter ","

for creating java classes from avro schema we need to build projects


Build:
gradle build

Running:
should run the main method in Application class

