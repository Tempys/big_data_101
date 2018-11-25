package com.dubovskyi.hdfs;

import java.io.IOException;
import java.util.Properties;


public class Application {

    public static void main(String[] args) throws IOException {


        Properties properties =new Properties();
        properties.load(Application.class.getClassLoader().getResourceAsStream("application.properties"));

        final String[] from = properties.getProperty("from").split(",");
        final String[] to = properties.getProperty("to").split(",");
        final String[] schema = properties.getProperty("avro.schema.path").split(",");

        HdfsService hdfsService = new HdfsService();
       // hdfsService.saveInAvro("hdfs://sandbox-hdp.hortonworks.com:8020/test/test.csv","hdfs://sandbox-hdp.hortonworks.com:8020/testavro/test.avro","test.avsc");

        for(int i =0; i<from.length;i++){
            System.out.println(schema[i]);
            hdfsService.saveInAvro(from[i],to[i],schema[i]);
        }




    }




}
