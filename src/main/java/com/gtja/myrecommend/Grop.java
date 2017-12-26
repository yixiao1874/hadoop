package com.gtja.myrecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Grop {
    public static class Step1_ToItemPreMapper extends Mapper<LongWritable,Text, Text, IntWritable>{

    }
}
