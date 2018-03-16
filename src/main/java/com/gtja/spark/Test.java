package com.gtja.spark;

import com.gtja.recommendjava.Person;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        String s = "Rating(10214554,454616,1.5622E-5)";
        String[] strings = s.split(",");
        /*System.out.println(strings[0].substring(7));
        System.out.println(strings[2].substring(0,strings[2].length()-1));*/
        Rating rating = new Rating(Integer.parseInt(strings[0].substring(7)), Integer.parseInt(strings[1]),
                Double.parseDouble(strings[2].substring(0,strings[2].length()-1)));
        System.out.println(rating.rating());
    }

}

