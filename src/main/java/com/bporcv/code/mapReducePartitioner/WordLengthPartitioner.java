package com.bporcv.code.mapReducePartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Administrator
 * @createDate 2020/7/17-14:05
 */
public class WordLengthPartitioner extends Partitioner<Text, LongWritable> {

    /**
     * 依据单词长度进行分区
     *      分区0：单词长度大于等于5
     *      分区1：单词长度小于5
     * @param key
     * @param value
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text key, LongWritable value, int numPartitions) {
        String words = key.toString();
        return words.length() >= 5 ? 0 : 1;
    }

}
