package com.gtja.recommendjava;

import scala.Tuple2;



import org.apache.spark.api.java.*;

import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.recommendation.ALS;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import org.apache.spark.mllib.recommendation.Rating;

import org.apache.spark.SparkConf;

// $example off$



public class JavaRecommendationExample {

    public static void main(String args[]) {

        // $example on$

        SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example").setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);



        // Load and parse the data

        String path = "file:///E:/test/test.dat";

        JavaRDD<String> data = jsc.textFile(path);

        JavaRDD<Rating> ratings = data.map(

                new Function<String, Rating>() {

                    public Rating call(String s) {

                        String[] sarray = s.split(",");

                        //sarray ={sarray[0],sarray[1],sarray[3]};

                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),

                                Double.parseDouble(sarray[2]));

                    }

                }

        );



        // Build the recommendation model using ALS

        int rank = 10;

        int numIterations = 10;

        //使用具体评分数进行训练

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



        // Save and load model

        model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");

        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),

                "target/tmp/myCollaborativeFilter");



        //使用模型为用户推荐内容

        Rating[] recommendations =sameModel.recommendProducts(1, 3);

        for(int i=0;i<recommendations.length;i++){

            System.out.println("推荐的产品:"+recommendations[i].product());

        }

        // $example off$

    }

}
