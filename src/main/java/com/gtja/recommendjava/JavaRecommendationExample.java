package com.gtja.recommendjava;

import com.gtja.mahout.Recommend;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;


public class JavaRecommendationExample {
    public static void main(String args[]) {
        String[] strings = {"out/artifacts/hadoop_jar/hadoop.jar"};
        SparkConf conf = new SparkConf().setAppName("Java Recommend")
                .setJars(strings);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(jsc);
        String url = "jdbc:mysql://192.168.56.1:3306/test?useSSL=false&autoReconnect=true&failOverReadOnly=false";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","1874");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");


        // Load and parse the data
        String path = args[0];
        //RDD<String> rdd = sqlContext.sparkContext().textFile(path,100);
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

        RDD<Rating> rdd = JavaRDD.toRDD(ratings);
        // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 10;

        //使用具体评分数进行训练
        /*train()参数详解
         * RDD<ratings>:原始的评分矩阵
         * rank:模型中隐语义因子个数
         * iterations:迭代次数
         * lambda:正则化参数，防止过度拟合
         */
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        //忽略评分数据进行模型训练
        //MatrixFactorizationModel model = ALS.trainImplicit(JavaRDD.toRDD(ratings), rank, numIterations, 0.01, 0.01);
        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );

        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                ));

        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(ratings.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(predictions).values();

        double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();
        System.out.println("Mean Squared Error = " + MSE);

        //给所有用户推荐
        RDD<Tuple2<Object,Rating[]>> recommendRDD = model.recommendProductsForUsers(10);


        Tuple2<Object,Rating[]>[] tuple2 = (Tuple2<Object,Rating[]>[])recommendRDD.take(2);
        for(Tuple2<Object,Rating[]> t :tuple2){
            Rating[] rating = t._2;
            for(Rating r:rating){
                System.out.println("为用户"+r.user()+r.product()+r.product());
            }
        }
        recommendRDD.saveAsTextFile("file:///E:/test/recommend.csv");

        //使用模型为用户推荐内容
        /*Rating[] recommendations =model.recommendProducts(1, 3);
        for(int i=0;i<recommendations.length;i++){
            System.out.println("推荐的产品为:"+recommendations[i].product());
            System.out.println("被推荐用户:"+recommendations[i].user());
            System.out.println("产品打分:"+recommendations[i].rating());

            //写入的数据内容
            JavaRDD<String> personData =
                    jsc.parallelize(Arrays.asList(recommendations[i].user()+" "+recommendations[i].product()+" "+recommendations[i].rating()));

            //第一步：在RDD的基础上创建类型为Row的RDD
            //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
            JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
                public Row call(String line) throws Exception {
                    String[] splited = line.split(" ");
                    return RowFactory.create(Integer.valueOf(splited[0]),Integer.valueOf(splited[1]),Double.valueOf(splited[2]));
                }
            });

        }*/

        // Save and load model
        //save()将模型存储在指定位置，存储的结果可以在下次读取时，直接执行上面的推荐函数，给出推荐结果。
        model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(), "target/tmp/myCollaborativeFilter");

    }
}
