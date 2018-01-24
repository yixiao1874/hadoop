package com.gtja.recommendjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TestJar {
    public static void main(String[] args) {
        String[] strings = {"out/artifacts/hadoop_jar/hadoop.jar"};
        SparkConf conf = new SparkConf().setAppName("Java Recommend")
                .setJars(strings);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setJobGroup("1","Java Recommend");
        jsc.stop();
    }
}
