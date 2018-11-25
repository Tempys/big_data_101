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

        for(int i =0; i<from.length;i++){
            System.out.println(schema[i]);
            hdfsService.saveInAvro(from[i],to[i],schema[i]);
        }




    }




}
