package com.gtja.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;
import shapeless.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SecondarySort {
    public static void main(String[] args) {
        String input = "file:///E:/test/secondarysort.txt";
        SparkConf conf = new SparkConf().setAppName(" secondary sort").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> lines = ctx.textFile(input);

        JavaPairRDD<String, Tuple2<Integer,Integer>> pairs =
                lines.mapToPair(new PairFunction<
                        String,
                        String,
                        Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Integer, Integer>> call(String s) throws Exception {
                        String[] tokens = s.split(" ");
                        System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                        Integer time = new Integer(tokens[1]);
                        Integer value = new Integer(tokens[2]);
                        Tuple2<Integer,Integer> timevalue =
                                new Tuple2<Integer,Integer>(time,value);
                        return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0],timevalue);
                    }
                });

        List<Tuple2<String, Tuple2<Integer, Integer>>> outpt = pairs.collect();
        for(Tuple2 t:outpt){
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._2);
        }

        JavaPairRDD<String,Iterable<Tuple2<Integer,Integer>>> groups = pairs.groupByKey();

        List<Tuple2<String,Iterable<Tuple2<Integer,Integer>>>> output2 = groups.collect();
        for(Tuple2<String,Iterable<Tuple2<Integer,Integer>>> t:output2){
            Iterable<Tuple2<Integer,Integer>> list = t._2;
            System.out.println(t._1);
            for(Tuple2<Integer,Integer> t2:list){
                System.out.println(t2._1 + "," + t2._2);
            }
        }

        JavaPairRDD<String,Iterable<Tuple2<Integer,Integer>>> sorted =
                groups.mapValues(
                        new Function<Iterable<Tuple2<Integer, Integer>>,
                                Iterable<Tuple2<Integer, Integer>>>() {
                            @Override
                            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> v1) throws Exception {
                                List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>((Collection<? extends Tuple2<Integer, Integer>>) v1);
                                //Collections.sort(newList,new TupleComparator());
                                return newList;
                            }
                        }
                );
        List<Tuple2<String,Iterable<Tuple2<Integer,Integer>>>> output3 = sorted.collect();
        for(Tuple2<String,Iterable<Tuple2<Integer,Integer>>> t:output3){
            Iterable<Tuple2<Integer,Integer>> list = t._2;
            System.out.println(t._1);
            for(Tuple2<Integer,Integer> t2:list){
                System.out.println(t2._1 + "," + t2._2);
            }
        }
    }
}
