package com.gtja.recommendjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;

public class TestJar {
    public static void main(String[] args) {
        String[] strings = {"out/artifacts/hadoop_jar/hadoop.jar"};
        SparkConf conf = new SparkConf().setAppName("Java Recommend");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String path =args[0];

        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Rating> ratings = data.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );

        ratings.saveAsTextFile("/user/text");

        JavaRDD<String> javaRDD = jsc.textFile("/user/text");
        JavaRDD<Rating> ratings2 = data.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0].substring(7)), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2].substring(0,sarray[2].length()-1)));
                    }
                }
        );

        System.out.println(ratings2.first().user());

    }
}
