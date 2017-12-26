package com.gtja.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Test {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //执行远端文件
        //conf.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
        //conf.set("dfs.replication", "2");//默认为3
        /*//远端执行
        conf.set("mapreduce.job.jar", "target/hadoop.jar");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        conf.set("mapreduce.app-submission.cross-platform", "true");*/
        Job job = Job.getInstance(conf);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //执行本地文件
        FileInputFormat.setInputPaths(job, "F:/Hadoop/test/input/test.txt");
        FileOutputFormat.setOutputPath(job, new Path("F:/Hadoop/test/output/"));

        /*//执行远端文件
        FileInputFormat.setInputPaths(job, "/input/test.txt");
        FileOutputFormat.setOutputPath(job, new Path("/output3/"));*/

       /* //远端执行
        FileInputFormat.setInputPaths(job, "/wcinput/");
        FileOutputFormat.setOutputPath(job, new Path("/wcoutput3/"));*/

        job.waitForCompletion(true);
    }
}