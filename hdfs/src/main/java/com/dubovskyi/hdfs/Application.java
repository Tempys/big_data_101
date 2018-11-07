package com.dubovskyi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Application {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration ();
        conf.set("fs.default.name", "hdfs://localhost:9000");

        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path("hdfs://localhost:9000/destination");

        FSDataInputStream in = hdfs.open(path);
        BufferedReader br=new BufferedReader(new InputStreamReader(in));
        try {
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);

                // be sure to read the next line otherwise you'll get an infinite loop
                line = br.readLine();
            }
        } finally {
            // you should close out the BufferedReader
            br.close();
        }






    }
}
