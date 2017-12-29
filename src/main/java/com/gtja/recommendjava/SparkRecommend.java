package com.gtja.recommendjava;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

public class SparkRecommend {
    public static void main(String[] args) {
        //步骤2：处理输入参数
        if(args.length < 1){
            System.err.println("参数错误，请输入正确文件位置");
            System.exit(1);
        }

        String userRatingsInputFile = args[0];
        System.out.println("usersRatingsInputFile = " + userRatingsInputFile);

        //步骤3：创建Spark上下文对象
        JavaSparkContext ctx = new JavaSparkContext();

        //步骤4：读取文件创建RDD
        JavaRDD<String> userRatings = ctx.textFile(userRatingsInputFile,1);

        //步骤5：找出谁曾对电影评分
        JavaPairRDD<String,Tuple2<String,Integer>> moviesRDD =
                userRatings.mapToPair(
                        new PairFunction<String, String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Tuple2<String, Integer>> call(String s) throws Exception {
                                String[] record = s.split(",");
                                String user = record[0];
                                String movie = record[1];
                                Integer rating = new Integer(record[2]);
                                Tuple2<String,Integer> userAndRating =
                                        new Tuple2<String, Integer>(user,rating);
                                return new Tuple2<String,Tuple2<String, Integer>>(movie,userAndRating);
                            }
                        }
                );

        //调用步骤5
        System.out.println("== debug1:moviesRDD: K = <movie>, " +
                            "= Tuple2<user,rating> ==");
        List<Tuple2<String,Tuple2<String,Integer>>> debug1 =
                moviesRDD.collect();
        for(Tuple2<String,Tuple2<String,Integer>> t2:debug1){
            System.out.println("debug1 key=" +t2._1 + "\t value = " + t2._2);
        }

        //步骤6：按movie对moviesRDD进行分组
        JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> moviesGroup =
                moviesRDD.groupByKey();

        System.out.println("== debug2:moviesGroup: K = <movie>, " +
                             "= Iterable<Tuple2<String,Integer>> ==");

        List<Tuple2<String,Iterable<Tuple2<String,Integer>>>> debug2 =
                moviesGroup.collect();
        for(Tuple2<String,Iterable<Tuple2<String,Integer>>> t2:debug2){
            System.out.println("debug1 key=" +t2._1 + "\t value = " + t2._2);
        }

        //步骤7：找出每个电影的评分人数
        JavaPairRDD<String, Tuple3<String,Integer,Integer>> usersRDD =
                moviesGroup.flatMapToPair(new PairFlatMapFunction<
                        Tuple2<String, Iterable<Tuple2<String, Integer>>>,
                        String,
                        Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Iterator<Tuple2<String, Tuple3<String, Integer, Integer>>> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> s) throws Exception {
                        List<Tuple2<String,Integer>> listOfUsersAndRatings =
                            new ArrayList<Tuple2<String,Integer>>();
                        //读取输入，并生成所需的（K，V）对
                        String movie = s._1;
                        Iterable<Tuple2<String,Integer>> pairsOfUserAndRating = s._2;
                        int numberOfRatings = 0;
                        for(Tuple2<String,Integer> t2 :pairsOfUserAndRating){
                            numberOfRatings++;
                            listOfUsersAndRatings.add(t2);
                        }
                        //发出（K，V）对
                        List<Tuple2<String,Tuple3<String,Integer,Integer>>> results =
                                new ArrayList<Tuple2<String,Tuple3<String,Integer,Integer>>>();
                        for(Tuple2<String,Integer> t2:listOfUsersAndRatings){
                            String user = t2._1;
                            Integer rating = t2._2;
                            Tuple3<String,Integer,Integer> t3 = new Tuple3<String,Integer,Integer>(movie,rating,numberOfRatings);
                                results.add(new Tuple2<String, Tuple3<String, Integer,Integer>>(user,t3));
                        }
                        return null;
                    }
                });
    }
}
