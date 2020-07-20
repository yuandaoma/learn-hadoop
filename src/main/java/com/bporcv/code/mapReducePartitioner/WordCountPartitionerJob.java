package com.bporcv.code.mapReducePartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Administrator
 * @createDate 2020/7/16-9:36
 */
public class WordCountPartitionerJob extends Configured implements Tool {

    public static final String HADOOP_URI = "hdfs://node1:8020";
    public static final String PATH = "/wordCountPartitioner";
    public static final String PARTITIONER_FILE = "data.txt";
    public static final String OUTPUT_DIR = "/output/partitioner";

    @Override
    public int run(String[] args) throws Exception {
        try {
            // 创建一个任务
            Job job = Job.getInstance(super.getConf(), WordCountPartitionerJob.class.getSimpleName());
            // 打包到集群上运行，必须要指定运行的主类
            job.setJarByClass(WordCountPartitionerJob.class);
            // 第一步，读取输入文件并解析成<K,V>键值对
            job.setInputFormatClass(TextInputFormat.class);
            // 为当前任务绑定所执行的文件信息
            TextInputFormat.addInputPath(job, new Path(HADOOP_URI + PATH));
            // 第二步：设置自定义mapper类
            job.setMapperClass(WordCountPartitionerMapper.class);
            // 设置自定义mapper处理输入的<K,V>之后的输出类型，用于reduce阶段使用
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // 第三步（分区），第四步（排序），第五步（初步规约），第六步（分组） 采用默认设置

            // 设置分区
            job.setPartitionerClass(WordLengthPartitioner.class);
            job.setNumReduceTasks(2);

            // 第七步：设置自定义reducer
            job.setReducerClass(WordCountPartitionerReducer.class);
            // 设置reduce阶段完成后的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // 设置输出类以及输出路径
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path(PATH + OUTPUT_DIR));
            boolean isComplete = job.waitForCompletion(true);
            return isComplete ? 0 : 1;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("===============================单词计数异常");
            return 0;
        }
    }

    public static void main(String[] args) {
        try {

            generatePartitionerData();

            Tool tool = new WordCountPartitionerJob();
            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration,tool,args);
            System.out.println("程序运行结束，即将退出.....");
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("程序运行异常");
        }
    }

    private static void generatePartitionerData() {
        try {
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FileSystem.get(new URI(HADOOP_URI), configuration);
            fileSystem.delete(new Path(PATH),true);
            String text = "hello,hadoop\nhello,world\njava,bigdata\nc,c++\npython,hive";
            FSDataOutputStream os = fileSystem.create(new Path(PATH + File.separator+ PARTITIONER_FILE));
            os.write(text.getBytes());
            os.flush();
            fileSystem.close();
            System.out.println("分区文件创建成功");
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            System.out.println("测试数据生成失败");
        }

    }
}
