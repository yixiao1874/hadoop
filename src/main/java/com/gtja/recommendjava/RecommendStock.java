package com.gtja.recommendjava;

import com.gtja.spark.CustAndStkInfo;
import com.gtja.spark.UtilityFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class RecommendStock {
    public static void main(String[] args) {
        String[] strings = {"out/artifacts/recommend_stock/hadoop.jar"};
        SparkConf conf = new SparkConf().setAppName("Java Recommend");
                ///.setJars(strings);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String path = args[0];

        JavaRDD<String> data = jsc.textFile(path);

        JavaRDD<CustAndStkInfo> custRDD = data.map(
                new Function<String, CustAndStkInfo>() {
                    public CustAndStkInfo call(String s) {
                        String[] sarray = s.split(",");
                        CustAndStkInfo custAndStkInfo = new CustAndStkInfo(
                                Integer.parseInt(sarray[0]),
                                Integer.parseInt(sarray[1]),
                                new BigDecimal(sarray[2]),Integer.parseInt(sarray[3]),
                                Integer.parseInt(sarray[4]),Integer.parseInt(sarray[5]),new BigDecimal(sarray[6]),Integer.parseInt(sarray[7]),
                                Integer.parseInt(sarray[8]));
                        return custAndStkInfo;
                    }
                }
        );

        JavaRDD<Rating> ratings = custRDD.map(
                new Function<CustAndStkInfo, Rating>() {
                    public Rating call(CustAndStkInfo custAndStkInfo) {
                        double hold_stock_ratio = holdStockRatio(custAndStkInfo.getHold_asset_avg().doubleValue(), custAndStkInfo.getHold_asset().doubleValue());
                        double hold_days_ratio = holdDaysRatio(custAndStkInfo.getHold_days(), custAndStkInfo.getTotal_hold_days());
                        double buy_times_ratio = buyTimesRatio(custAndStkInfo.getMatch_cnt(), custAndStkInfo.getTotal_match_cnt());
                        double buy_lastdate_ratio = buyLastdateRatio(custAndStkInfo.getIn_match_lastdate(), new Date());
                        double score = compileScore(hold_stock_ratio, hold_days_ratio, buy_times_ratio, buy_lastdate_ratio);
                        //Row row = RowFactory.create(custAndStkInfo.getCustomer_id(),custAndStkInfo.getStock_id(), score);
                        return new Rating(custAndStkInfo.getCustomer_id(),custAndStkInfo.getStock_id(), score);
                    }

                    public double holdStockRatio(double holdAvgAsset, double holdAsset) {
                        double hold_stock_ratio = 0;
                        if(holdAsset != 0)
                            hold_stock_ratio = holdAvgAsset / holdAsset;
                        return hold_stock_ratio;
                    }

                    public double holdDaysRatio(int holdAvgDays, int holdDays) {
                        double hold_days_ratio = 0;
                        if(holdDays != 0)
                            hold_days_ratio = (double)holdAvgDays / (double)holdDays;
                        return hold_days_ratio;
                    }

                    public double buyTimesRatio(int buyAvgTimes, int buyTimes) {
                        double buy_times_ratio = 0;
                        if(buyTimes != 0)
                            buy_times_ratio = (double)buyAvgTimes / (double)buyTimes;
                        return buy_times_ratio;
                    }

                    public double buyLastdateRatio(int buyLastdate, Date currentDate) {
                        double buy_lastdate_ratio = 0;
                        if (buyLastdate != 0) {
                            int daylength = 0;
                            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                            try {
                                daylength = UtilityFunction.longOfTwoDate(format.parse(buyLastdate+""), currentDate);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            if(daylength != 0)
                                buy_lastdate_ratio = 1 / (double)daylength;
                        }
                        return buy_lastdate_ratio;
                    }

                    public double compileScore(double holdStockRatio, double holdDaysRatio, double buyTimesRatio,
                                               double buyLastdateRatio) {
                        double score = holdStockRatio * holdDaysRatio * buyTimesRatio * buyLastdateRatio;
                        return score;
                    }
                }
        );

        int rank = Integer.parseInt(args[1]);
        int numIterations = Integer.parseInt(args[2]);
        double lambda = Double.parseDouble(args[3]);

        //使用具体评分数进行训练
        /*train()参数详解
         * RDD<ratings>:原始的评分矩阵
         * rank:模型中隐语义因子个数
         * iterations:迭代次数
         * lambda:正则化参数，防止过度拟合
         *  rank  = 10
            模型的潜在因素的个数，即“用户 - 特征”和“产品 - 特征”矩阵的列数；一般来说，
            它也是矩阵的阶。
            iterations  = 5
            矩阵分解迭代的次数；迭代的次数越多，花费的时间越长，但分解的结果可能会更好。
            lambda  = 0.01
            标准的过拟合参数；值越大越不容易产生过拟合，但值太大会降低分解的准确度。
            alpha  = 1.0
            控制矩阵分解时，被观察到的“用户 - 产品”交互相对没被观察到的交互的权重。
         */
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, lambda);

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

    }
}
