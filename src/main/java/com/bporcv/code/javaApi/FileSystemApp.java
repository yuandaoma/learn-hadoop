package com.bporcv.code.javaApi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;


/**
 * @author Administrator
 * @createDate 2020/7/15-18:05
 */
public class FileSystemApp {


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration);
        System.out.println(fileSystem.toString());
    }
}
