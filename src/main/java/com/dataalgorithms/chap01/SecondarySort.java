package com.dataalgorithms.chap01;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SecondarySort {
    public static void main(String[] args) {
        /**
         * x 2 9
         * y 2 5
         */
        SparkConf conf = new SparkConf().setAppName("SecondarySort");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        String inputPath = args[0];

        JavaRDD<String> lines = jsc.textFile(inputPath);

        //<x,<2,9>>
        JavaPairRDD<String, Tuple2<Integer,Integer>> pairs =
                lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                        String[] tokens = s.split(" ");
                        Tuple2<Integer,Integer> timevalue =
                                new Tuple2<Integer,Integer>(new Integer(tokens[1]),new Integer(tokens[2]));
                        return new Tuple2<String,Tuple2<Integer, Integer>>(tokens[0],timevalue);
                    }
                });

        List<Tuple2<String,Tuple2<Integer, Integer>>> output = pairs.collect();
        for(Tuple2 t:output){
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 +"," + timevalue._2);
        }

        //按键分组
        JavaPairRDD<String, Iterable<Tuple2<Integer,Integer>>> groups = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<Integer,Integer>>> sorted = groups.mapValues(
                new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
                    @Override
                    public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {
                        //对v1排序
                        List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>((Collection<? extends Tuple2<Integer, Integer>>) v1);
                        return null;
                    }
                }
        );
    }
}
