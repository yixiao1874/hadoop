package com.gtja.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*JavaRDD<String> input = sc.textFile(
                "file:///E:/test/test.dat");
        JavaRDD<String> words = input.flatMap(
          s -> Arrays.asList(s.split(",")).iterator()
        );

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        sc.stop();*/

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 3));
        /*JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
            public Integer call(Integer x) { return x*x; }
        });*/
    }
}
