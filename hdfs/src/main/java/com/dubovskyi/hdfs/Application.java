package com.dubovskyi.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;



public class Application {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration ();
      //  conf.set("dfs.client.use.datanode.hostname","true");
     //   conf.set ("dfs.client.use.datanode.hostname","true");
     //   conf.set("dfs.datanode.use.datanode.hostname","true");
     //   conf.set("dfs.client.use.legacy.blockreader", "true");

       // String path1 = Application.class.getResource("/core-site.xml").getFile();
      //+  conf.addResource(new Path(path1));
      //  String path2 = Application.class.getResource("/hdfs-site.xml").getFile();

     //   conf.addResource(new Path(path1));
    //    conf.addResource(new Path(path2));
     //  conf.set("fs.default.name", "hdfs://localhost:9000");

        FileSystem hdfs = FileSystem.get(conf);
       // Path path = new Path("hdfs://sandbox-hdp.hortonworks.com:8020/destination/");

        Schema destinationSchema = parseSchema();

          FileStatus[] a= hdfs.listStatus(new Path("hdfs://sandbox-hdp.hortonworks.com:8020/"));


        for(FileStatus status : a){
           System.out.println(status.getPath().toString());
        }
         int count=0;
     hdfs.copyFromLocalFile(new Path("C:\\test\\destinations.csv"),new Path("hdfs://sandbox-hdp.hortonworks.com:8020/destination2/destinations2.csv"));

        FSDataInputStream in = hdfs.open(new Path("hdfs://sandbox-hdp.hortonworks.com:8020/destination2/destinations2.csv"));
        OutputStream out = hdfs.create(new Path("hdfs://sandbox-hdp.hortonworks.com:8020/destinationavro1/destinations.avro"));


        BufferedReader br=new BufferedReader(new InputStreamReader(in));
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(destinationSchema);
        DataFileWriter<GenericRecord> dataFileWriter = null;

        try {
            dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
            dataFileWriter.create(destinationSchema, out);
            String line;
            line=br.readLine();
            String[] names = line.split(",");
           System.out.println(line);
            while (line != null){

                GenericRecord person1 = new GenericData.Record(destinationSchema);
                // be sure to read the next line otherwise you'll get an infinite loop
                line = br.readLine();

                if(line != null){
                    GenericRecord  genericRecord = createGenericRecord(line,names,destinationSchema);
                    dataFileWriter.append(genericRecord);
                }



            }
        } finally {
            // you should close out the BufferedReader
            br.close();

            if(dataFileWriter != null) {
                try {
                    dataFileWriter.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }




    }

    private  static  GenericRecord createGenericRecord(String line,String[] names,Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        System.out.println(line);

        String[] values = line.split(",");

        for(int i =0;i<values.length;i++){

            if(i==0) { record.put(names[i],Long.parseLong(values[i]));}
            else {record.put(names[i],Double.parseDouble(values[i])); }

        }

        return record;
    }

    protected static Schema parseSchema() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            // Path to schema file
            schema = parser.parse(ClassLoader.getSystemResourceAsStream("destinations.avsc"));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }
}
