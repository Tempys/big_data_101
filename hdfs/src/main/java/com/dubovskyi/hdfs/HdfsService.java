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

    public void saveInAvro(String from, String to, String schema) throws IOException {

        Configuration conf = new Configuration ();
        FileSystem hdfs = FileSystem.get(conf);
        Schema destinationSchema = SchemaHelper.parseSchema(schema);

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
               String[] names = line.split(",");

               while (line != null) {

                     line = br.readLine();

                     if (line != null) {

                       GenericRecord genericRecord = createGenericRecord(line, names, destinationSchema);
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

    protected   GenericRecord createGenericRecord(String line,String[] names,Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        System.out.println(line);

        String[] values = line.split(",");

        for(int i =0;i<values.length;i++){

            Object value = toAvro(names[i],values[i],schema);
            record.put(names[i],value);
        }

        return record;
    }

    private Object toAvro(String name,String value,Schema schema)   {

        Schema.Type type = schema.getField(name).schema().getTypes().get(1).getType();

        switch (type){
            case LONG:  return Long.parseLong(value);
            case DOUBLE: return  Double.parseDouble(value);
            default: throw new IllegalArgumentException(String.format("mapping for this type %s is missing",type));
        }

    }
}
