package com.bporcv.code.mapReducePartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Administrator
 * @createDate 2020/7/16-9:36
 */
public class WordCountPartitionerMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");
        Text keyText = new Text();
        LongWritable valueLong = new LongWritable();
        for (String word : words) {
            keyText.set(word);
            valueLong.set(1);
            context.write(keyText,valueLong);
        }
    }
}
