package com.dubovskyi.hdfs;

import org.apache.avro.Schema;

import java.io.IOException;

public class SchemaHelper {



    public static Schema parseSchema(String path) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            // Path to schema file
            schema = parser.parse(ClassLoader.getSystemResourceAsStream(path));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }
}
