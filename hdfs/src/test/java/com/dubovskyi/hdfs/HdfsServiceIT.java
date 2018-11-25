package com.dubovskyi.hdfs;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class HdfsServiceIT {


    /**
     * only for testing purpose
     */
    @Test
    @Ignore
    public void copyfromLocalFileToHdfs() throws IOException {
        HdfsService hdfsService = new HdfsService();
        hdfsService.saveFromLocalToHdfs("C:\\projects\\cources\\all\\train.csv","hdfs://sandbox-hdp.hortonworks.com:8020/train/trains.csv");
    }

}
