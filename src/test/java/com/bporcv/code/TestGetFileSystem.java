package com.bporcv.code;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.net.URI;

/**
 * @author Administrator
 * @createDate 2020/7/15-20:16
 */
public class TestGetFileSystem {

    @Test
    public void testGetFileSystemMethodOne() throws Exception {
        // 获取配置对象
        Configuration configuration = new Configuration();
        // 设置Configuration对象，设置要操作的文件系统,fs.defaultFs为之前core-site.xml中的配置信息
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        // 通过配置信息获取到hadfs的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        //打印获取到的文件系统
        System.out.println(fileSystem.toString());
    }

    @Test
    public void testGetFileSystemMethodTwo() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void testGetFileSystemMethodThree() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), configuration);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void testGetFileSystemMethodFour() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node1:8020"), configuration);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void testHdfsUploadFile() {

        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://node1:8020");
            FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
            //将本地文件系统D盘下的hadoop.txt上传到HDFS文件系统的/目录下
            fileSystem.copyFromLocalFile(new Path("D://hadoop.txt"), new Path("/"));
            fileSystem.close();
            System.out.println("文件上传成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("文件上传失败");
        }
    }

    @Test
    public void testHdfsCreateDir() {

        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://node1:8020");
            FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
            fileSystem.create(new Path("/learn/hadoop"));
            fileSystem.close();
            System.out.println("文件夹创建成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("文件夹创建失败");
        }
    }

    @Test
    public void testHdfsFileDownload() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://node1:8020");
            FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
            // 将HDFS文件系统中的/hadoop.txt下载到本地文件系统中的D盘下，名称为hadoop2.txt
            fileSystem.copyToLocalFile(new Path("/hadoop.txt"),new Path("D://hadoop2.txt"));
            fileSystem.close();
            System.out.println("文件下载成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("文件下载失败");
        }
    }

    @Test
    public void testHdfsFileDelete() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://node1:8020");
            FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
            // 删除/hadoop.txt文件，第二个参数标识是否递归删除，只有当第一个参数是文件夹的，第二个参数才会生效
            fileSystem.delete(new Path("/hadoop.txt"),false);
            fileSystem.close();
            System.out.println("文件删除成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("文件删除失败");
        }
    }

    /**
     * 遍历hdfs中的文件
     */
    @Test
    public void testHdfsFileList() {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS", "hdfs://node1:8020");
            FileSystem fileSystem = FileSystem.get(new URI("/"), configuration);
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
            while (iterator.hasNext()){
                System.out.println("=================================================================");
                LocatedFileStatus next = iterator.next();
                System.out.println("next.getPermission() = " + next.getPermission());
                System.out.println("next.getOwner() = " + next.getOwner());
                System.out.println("next.getGroup() = " + next.getGroup());
                System.out.println("next.getBlockSize() = " + next.getBlockSize());
                System.out.println("next.getModificationTime() = " + next.getModificationTime());
                System.out.println("next.getReplication() = " + next.getReplication());
                System.out.println("next.getPath( = " + next.getPath());
                System.out.println("next.getPath().getName() = " + next.getPath().getName());
                System.out.println("=================================================================");
            }
            fileSystem.close();
            System.out.println("文件遍历成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("文件遍历失败");
        }
    }

    @Test
    public void testHdfsMergeSmallFile(){
        try {
            //获取分布式文件系统
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());
            FSDataOutputStream outputStream = fileSystem.create(new Path("/merge.txt"));
            //获取本地文件系统
            LocalFileSystem local = FileSystem.getLocal(new Configuration());
            //通过本地文件系统获取文件列表，为一个集合
            FileStatus[] fileStatuses = local.listStatus(new Path("T:\\smallfile\\"));
            for (FileStatus fileStatus : fileStatuses) {
                FSDataInputStream inputStream = local.open(fileStatus.getPath());
                IOUtils.copy(inputStream,outputStream);
                IOUtils.closeQuietly(inputStream);
            }
            IOUtils.closeQuietly(outputStream);
            local.close();
            fileSystem.close();
            System.out.println("小文件合并成功");
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("小文件合并失败");
        }
    }
}
