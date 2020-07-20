package com.bporcv.code.mapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Administrator
 * @createDate 2020/7/16-9:36
 */
public class WordCountJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            // 创建一个任务
            Job job = Job.getInstance(super.getConf(), WordCountJob.class.getSimpleName());
            // 打包到集群上运行，必须要指定运行的主类
            job.setJarByClass(WordCountJob.class);
            // 第一步，读取输入文件并解析成<K,V>键值对
            job.setInputFormatClass(TextInputFormat.class);
            // 为当前任务绑定所执行的文件信息
            TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/wordcount"));
            // 第二步：设置自定义mapper类
            job.setMapperClass(WordCountMapper.class);
            // 设置自定义mapper处理输入的<K,V>之后的输出类型，用于reduce阶段使用
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // 第三步（分区），第四步（排序），第五步（初步规约），第六步（分组） 采用默认设置
            // 第七步：设置自定义reducer
            job.setReducerClass(WordCountReducer.class);
            // 设置reduce阶段完成后的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // 设置输出类以及输出路径
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, new Path("hdfs://node1:8020/wordcount_out"));
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
            Tool tool = new WordCountJob();
            Configuration configuration = new Configuration();
            int run = ToolRunner.run(configuration,tool,args);
            System.out.println("程序运行结束，即将退出.....");
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("程序运行异常");
        }
    }
}
