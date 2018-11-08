package com.dubovskyi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class Application {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration ();

        String path1 = Application.class.getResource("/core-site.xml").getFile();
        System.out.println(path1);
        conf.addResource(new Path(path1));
        String path2 = Application.class.getResource("/hdfs-site.xml").getFile();

        System.out.println(path2);
        conf.addResource(new Path(path1));
        conf.addResource(new Path(path2));
     //  conf.set("fs.default.name", "hdfs://localhost:9000");

        FileSystem hdfs = FileSystem.get(conf);
        Path path = new Path("hdfs://sandbox-hdp.hortonworks.com:8020/");

        FileStatus[] a= hdfs.listStatus(path);

        for(FileStatus status : a){
            System.out.println(status);
        }

     /*   FSDataInputStream in = hdfs.open(path);
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
        }*/






    }
}
