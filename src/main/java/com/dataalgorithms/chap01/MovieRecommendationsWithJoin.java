package com.dataalgorithms.chap01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MovieRecommendationsWithJoin {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendationsWithJoin");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        String filePath = args[0];

        //user movie rating
        JavaRDD<String> usersRatings = jsc.textFile(filePath);
        //<movie,<user,rating>>
        JavaPairRDD<String, Tuple2<String,Integer>> moviesRDD =
                usersRatings.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                        String[] record = s.split(",");
                        String user = record[0];
                        String movie = record[1];
                        Integer rating = Integer.parseInt(record[2]);
                        Tuple2<String,Integer> userAndRating = new Tuple2<String,Integer>(user,rating);
                        return new Tuple2<String,Tuple2<String, Integer>>(movie,userAndRating);
                    }
                });
        //按movie对moviesRDD分组
        //101-----[(1,1), (2,5), (3,1), (4,1), (5,1), (5,1), (6,5), (7,1), (8,1), (9,1)]
        JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> moviesGrouped = moviesRDD.groupByKey();
        List<Tuple2<String,Iterable<Tuple2<String,Integer>>>> debug = moviesGrouped.collect();
        for(Tuple2<String,Iterable<Tuple2<String,Integer>>> tuple2:debug){
            System.out.println(tuple2._1+"-----"+tuple2._2);
        }

        //找出每部电影的评分人数
        JavaPairRDD<String, Tuple3<String,Integer,Integer>> usersRDD =
                moviesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Iterator<Tuple2<String, Tuple3<String, Integer, Integer>>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> s) throws Exception {
                        List<Tuple2<String,Integer>> listOfUsersAndRatings =
                                new ArrayList<Tuple2<String,Integer>>();
                        String movie = s._1;
                        Iterable<Tuple2<String,Integer>> pairsOfUserAndRating = s._2;
                        int numberOfRaters = 0;
                        for(Tuple2<String,Integer> t2:pairsOfUserAndRating){
                            numberOfRaters++;
                            listOfUsersAndRatings.add(t2);
                        }

                        List<Tuple2<String,Tuple3<String,Integer,Integer>>> results =
                                new ArrayList<Tuple2<String,Tuple3<String,Integer,Integer>>>();
                        for(Tuple2<String,Integer> t2:listOfUsersAndRatings){
                            String user = t2._1;
                            Integer rating = t2._2;
                            Tuple3<String,Integer,Integer> t3 =
                                    new Tuple3<String,Integer,Integer>(movie,rating,numberOfRaters);
                            results.add(new Tuple2<String, Tuple3<String,Integer,Integer>>(user,t3));
                        }
                        return (Iterator<Tuple2<String, Tuple3<String, Integer, Integer>>>) results;
                    }
                });

        List<Tuple2<String, Tuple3<String, Integer, Integer>>> debug2 = usersRDD.collect();
        for(Tuple2<String, Tuple3<String, Integer, Integer>> t2:debug2){
            System.out.println(t2._1 + "----" +t2._2);
        }
    }
}
