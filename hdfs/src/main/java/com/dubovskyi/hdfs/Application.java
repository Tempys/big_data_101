package com.dubovskyi.hdfs;

import java.io.IOException;
import java.util.Properties;


public class Application {

    public static void main(String[] args) throws IOException {


        Properties properties =new Properties();
        properties.load(Application.class.getClassLoader().getResourceAsStream("application.properties"));

        final String from = properties.getProperty("from");
        final String to = properties.getProperty("to");
        final String schema = properties.getProperty("avro.schema.path");

        HdfsService hdfsService = new HdfsService();

        hdfsService.saveInAvro(from,to,schema );


    }




}
