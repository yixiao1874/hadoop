package com.gtja.spark;

import com.gtja.recommendjava.Person;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("My App").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

    }

}

