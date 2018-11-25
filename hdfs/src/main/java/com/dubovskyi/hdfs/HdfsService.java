package com.dubovskyi.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;


/**
 * This is the main class for integration with hdfs
 */
public class HdfsService {

  private static final Logger log = LoggerFactory.getLogger(HdfsService.class);


    /**
     * this method upload destination.csv from hdfs and convert this data to avro file and save in hdfs
     * @param from - hdfs path to initial destination.csv
     * @param to - hadfs path of avro file
     * @param schema - avro schema for destination.csv
     * @throws IOException
     */
    public void saveInAvro(String from, String to, String schema) throws IOException {

        Configuration conf = new Configuration ();
        FileSystem hdfs = FileSystem.get(conf);
        Schema destinationSchema = AvroHelper.parseSchema(schema);

        FSDataInputStream in = hdfs.open(new Path(from));
        OutputStream out = hdfs.create(new Path(to));

       try( BufferedReader br=new BufferedReader(new InputStreamReader(in))) {

           DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(destinationSchema);
           DataFileWriter<GenericRecord> dataFileWriter = null;

           try {

               dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
               dataFileWriter.create(destinationSchema, out);
               String line;
               line = br.readLine();
              // System.out.println("line: "+ line);
               String[] names = line.split(",");

               while (line != null) {

                     line = br.readLine();


                     if (line != null) {

                       GenericRecord genericRecord = AvroHelper.createGenericRecord(line, names, destinationSchema);
                       dataFileWriter.append(genericRecord);

                     }
               }
           } finally {

               if (dataFileWriter != null) {
                   try {
                       dataFileWriter.close();
                   } catch (IOException e) {
                       log.error("can not close dataFileWriter: ",e);
                   }
               }
           }
       }
    }

    public void saveFromLocalToHdfs(String src,String dst) throws IOException {
        Configuration conf = new Configuration ();
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.copyFromLocalFile(new Path(src),new Path(dst));
    }




}
