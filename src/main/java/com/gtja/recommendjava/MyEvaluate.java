package com.gtja.recommendjava;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.RankingMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MyEvaluate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Java Recommend");
        //.setJars(strings);
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = args[0];
        //RDD<String> rdd = sqlContext.sparkContext().textFile(path,100);

        JavaRDD<String> data = sc.textFile(path);
        double[] d = {0.6,0.4};
        JavaRDD<String>[] splitRDD = data.randomSplit(d);
//训练集  60%
        JavaRDD<Rating> ratings = splitRDD[0].map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        /*return new Rating(Integer.parseInt(sarray[0].substring(7)), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2].substring(0,sarray[2].length()-1)));*/
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );
//测试集  40%
        JavaRDD<Rating> testRatings = splitRDD[1].map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        /*return new Rating(Integer.parseInt(sarray[0].substring(7)), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2].substring(0,sarray[2].length()-1)));*/
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );
        testRatings.cache();
        ratings.cache();

        int rank = Integer.parseInt(args[1]);
        int numIterations = Integer.parseInt(args[2]);
        double lambda = Double.parseDouble(args[3]);

// Train an ALS model
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, lambda);

// Get top 5 recommendations for every user and scale ratings from 0 to 1
        JavaRDD<Tuple2<Object, Rating[]>> userRecs = model.recommendProductsForUsers(5).toJavaRDD();

        JavaRDD<Tuple2<Object, Rating[]>> userRecsScaled = userRecs.map(t -> {
            Rating[] scaledRatings = new Rating[t._2().length];
            for (int i = 0; i < scaledRatings.length; i++) {
                double newRating = Math.max(Math.min(t._2()[i].rating(), 1.0), 0.0);
                scaledRatings[i] = new Rating(t._2()[i].user(), t._2()[i].product(), newRating);
            }
            return new Tuple2<>(t._1(), scaledRatings);
        });

        JavaPairRDD<Object, Rating[]> userRecommended = JavaPairRDD.fromJavaRDD(userRecsScaled);

// Map ratings to 1 or 0, 1 indicating a movie that should be recommended
        //将原始打分数据中的评分化为  0/1
        JavaRDD<Rating> binarizedRatings = testRatings.map(r -> {
            double binaryRating;
            if (r.rating() > 0.0) {
                binaryRating = 1.0;
            } else {
                binaryRating = 0.0;
            }
            return new Rating(r.user(), r.product(), binaryRating);
        });

// Group ratings by common user
        //binarizedRatings--->userMovies
        JavaPairRDD<Object, Iterable<Rating>> userMovies = binarizedRatings.groupBy(Rating::user);

// Get true relevant documents from all user ratings
        JavaPairRDD<Object, List<Integer>> userMoviesList = userMovies.mapValues(docs -> {
            List<Integer> products = new ArrayList<>();
            for (Rating r : docs) {
                if (r.rating() > 0.0) {
                    products.add(r.product());
                }
            }
            return products;
        });

// Extract the product id from each recommendation
        JavaPairRDD<Object, List<Integer>> userRecommendedList = userRecommended.mapValues(docs -> {
            List<Integer> products = new ArrayList<>();
            for (Rating r : docs) {
                products.add(r.product());
            }
            return products;
        });
        //<用户打分商品列表，推荐商品列表>
        JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = userMoviesList.join(
                userRecommendedList).values();

// Instantiate the metrics object
        RankingMetrics<Integer> metrics = RankingMetrics.of(relevantDocs);


// Precision and NDCG at k
        Integer[] kVector = {1, 3, 5};
        for (Integer k : kVector) {
            System.out.format("Precision at %d = %f\n", k, metrics.precisionAt(k));
            System.out.format("NDCG at %d = %f\n", k, metrics.ndcgAt(k));
        }

// Mean average precision
        System.out.format("Mean average precision = %f\n", metrics.meanAveragePrecision());

// Evaluate the model using numerical ratings and regression metrics
        JavaRDD<Tuple2<Object, Object>> userProducts =
                ratings.map(r -> new Tuple2<>(r.user(), r.product()));

        JavaPairRDD<Tuple2<Integer, Integer>, Object> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(r ->
                        new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())));
        JavaRDD<Tuple2<Object, Object>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(ratings.map(r ->
                        new Tuple2<Tuple2<Integer, Integer>, Object>(
                                new Tuple2<>(r.user(), r.product()),
                                r.rating())
                )).join(predictions).values();

// Create regression metrics object
        RegressionMetrics regressionMetrics = new RegressionMetrics(ratesAndPreds.rdd());

// Root mean squared error
        System.out.format("RMSE = %f\n", regressionMetrics.rootMeanSquaredError());

// R-squared
        System.out.format("R-squared = %f\n", regressionMetrics.r2());
    }
}
