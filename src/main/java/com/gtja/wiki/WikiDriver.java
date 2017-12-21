package com.gtja.wiki;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class WikiDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //执行远端文件
        conf.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
        //conf.set("dfs.replication", "2");//默认为3
        //远端执行
        conf.set("mapreduce.job.jar", "target/hadoop.jar");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job1 = Job.getInstance(conf);

        job1.setMapperClass(WikipediaToItemPrefsMapper.class);
        job1.setReducerClass(WikipediaToUserVectorReducer.class);

        job1.setMapOutputKeyClass(VarLongWritable.class);
        job1.setMapOutputValueClass(VarLongWritable.class);

        //将job1输出的文件格式设置为SequenceFileOutputFormat
        job1.setOutputKeyClass(VarLongWritable.class);
        job1.setOutputValueClass(VectorWritable.class);


        FileInputFormat.setInputPaths(job1, "/input/mywiki.txt");
        FileOutputFormat.setOutputPath(job1, new Path("/output/wikiout"));

        job1.waitForCompletion(true);


        Job job2 = Job.getInstance(conf);
        job2.setMapperClass(UserVectorToCooccurrenceMapper.class);
        job2.setReducerClass(UserVectorToCooccurrenceReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(VectorWritable.class);

        //将job2的输入文件格式设置为SequenceFileInputFormat
        FileInputFormat.setInputPaths(job1, "/output/wikiout");
        FileOutputFormat.setOutputPath(job2, new Path("/output/wikiout1"));

        job2.waitForCompletion(true);
        /*if (args.length!=2) {
            System.out.println("useage: <input dir> <output dir>");
            return 1;
        }
        OperatingFiles(args[0], args[1]);
        Configuration job1Conf = new Configuration();
        Job job1 = new Job(job1Conf, "job1");
        job1.setJarByClass(WikiDriver.class);
        job1.setMapperClass(WikipediaToItemPrefsMapper.class);
        job1.setReducerClass(WikipediaToUserVectorReducer.class);
        job1.setMapOutputKeyClass(VarLongWritable.class);
        job1.setMapOutputValueClass(VarLongWritable.class);

        //将job1输出的文件格式设置为SequenceFileOutputFormat
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setOutputKeyClass(VarLongWritable.class);
        job1.setOutputValueClass(VectorWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(TMP_PATH));

        Configuration job2Conf = new Configuration();
        Job job2 = new Job(job2Conf, "job2");
        job2.setJarByClass(WikiDriver.class);
        job2.setMapperClass(UserVectorToCooccurrenceMapper.class);
        job2.setReducerClass(UserVectorToCooccurrenceReducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(VectorWritable.class);

        //将job2的输入文件格式设置为SequenceFileInputFormat
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(TMP_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        ControlledJob ctrlJob1 = new ControlledJob(job1.getConfiguration());
        ctrlJob1.setJob(job1);

        ControlledJob ctrlJob2 = new ControlledJob(job2.getConfiguration());
        ctrlJob2.setJob(job2);

        ctrlJob2.addDependingJob(ctrlJob1);

        JobControl JC = new JobControl("wiki job");
        JC.addJob(ctrlJob1);
        JC.addJob(ctrlJob2);

        Thread thread = new Thread(JC);
        thread.start();
        while (true) {
            if(JC.allFinished()) {
                System.out.println(JC.getSuccessfulJobList());
                JC.stop();
                System.exit(0);
            }

            if (JC.getFailedJobList().size() > 0) {
                System.out.println(JC.getFailedJobList());
                JC.stop();
                System.exit(1);
            }
        }*/
    }

    /*//操作hdfs的文件夹
    static void OperatingFiles(String input, String output) {
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        Path tmpPath = new Path(TMP_PATH);
        Configuration conf = new Configuration();
        conf.addResource(new Path(HADOOP_HOME+"/core-site.xml"));
        conf.addResource(new Path(HADOOP_HOME+"/hdfs-site.xml"));
        try {
            FileSystem hdfs = FileSystem.get(conf);
            if (!hdfs.exists(inputPath)) {
                System.out.println("input path no exist!");
            }
            if (hdfs.exists(outputPath)) {
                System.out.println("output path:"+outputPath.toString()+ " exist, deleting this path...");
                hdfs.delete(outputPath,true);
            }
            if (hdfs.exists(tmpPath)) {
                System.out.println("tmp path:"+tmpPath.toString()+ " exist, deleting this path...");
                hdfs.delete(tmpPath,true);
            }
        } catch (Exception e) {

        }

    }*/
}
