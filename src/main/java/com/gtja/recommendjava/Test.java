package com.gtja.recommendjava;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Collaborative filtering 协同过滤 alternating least squares (ALS) (交替最小二乘法(ALS) )
 * Title. <br>
 * Description.
 * <p>
 * Version: 1.0
 * <p>
 */
public class Test {

    public static void main(String[] args) {
        // 创建入口对象
        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 加载评分数据
        String path = "file:///E:/test/MovieLens/ml-1m/ratings.dat";
        JavaRDD<String> data = sc.textFile(path);

        // 所有评分数据，由于此数据要分三部分使用，60%用于训练，20%用于验证，最后20%用于测试。将时间戳%10可以得到近似的10等分，用于三部分数据切分
        JavaRDD<Tuple2<Integer, Rating>> ratingsTrain_KV = data.map(new Function<String, Tuple2<Integer, Rating>>() {

            @Override
            public Tuple2<Integer, Rating> call(String line) throws Exception {
                String[] fields = line.split("::");
                if (fields.length != 4) {
                    throw new IllegalArgumentException("每一行必须有且只有4个元素");
                }
                int userId = Integer.parseInt(fields[0]);
                int movieId = Integer.parseInt(fields[1]);
                double rating = Float.parseFloat(fields[2]);
                int timestamp = (int) (Long.parseLong(fields[3])%10);
                return new Tuple2<Integer, Rating>(timestamp, new Rating(userId, movieId, rating));
            }

        });

        System.out.println("get " + ratingsTrain_KV.count() + " ratings from " + ratingsTrain_KV.distinct().count()
                + "users on " + ratingsTrain_KV.distinct().count() + "movies");

        // 加载我的评分数据
        String mypath = "file:///E:/test/MovieLens/ml-1m/test.dat";
        JavaRDD<String> mydata = sc.textFile(mypath);
        JavaRDD<Rating> myRatedData_Rating = mydata.map(new Function<String, Rating>() {

            @Override
            public Rating call(String line) throws Exception {
                String[] fields = line.split("::");
                if (fields.length != 4) {
                    throw new IllegalArgumentException("每一行必须有且只有4个元素");
                }
                int userId = Integer.parseInt(fields[0]);
                int movieId = Integer.parseInt(fields[1]);
                double rating = Float.parseFloat(fields[2]);
                return new Rating(userId, movieId, rating);
            }
        });

        //设置分区数
        int numPartitions = 3;

        //将键值小于6（60%）的数据用于训练
        JavaRDD<Rating> traningData_Rating = JavaPairRDD.fromJavaRDD(ratingsTrain_KV.filter(new Function<Tuple2<Integer,Rating>, Boolean>() {

            @Override
            public Boolean call(Tuple2<Integer, Rating> v1) throws Exception {

                return v1._1 < 6;
            }
        })).values().union(myRatedData_Rating).repartition(numPartitions).cache();

        //将键值大于6小于8（20%）的数据用于验证
        JavaRDD<Rating> validateData_Rating = JavaPairRDD.fromJavaRDD(ratingsTrain_KV.filter(new Function<Tuple2<Integer,Rating>, Boolean>() {

            @Override
            public Boolean call(Tuple2<Integer, Rating> v1) throws Exception {

                return v1._1 >= 6 && v1._1 < 8;
            }
        })).values().repartition(numPartitions).cache();

        //将键值大于8（20%）的数据用于测试
        JavaRDD<Rating> testData_Rating = JavaPairRDD.fromJavaRDD(ratingsTrain_KV.filter(new Function<Tuple2<Integer,Rating>, Boolean>() {

            @Override
            public Boolean call(Tuple2<Integer, Rating> v1) throws Exception {

                return v1._1 >= 8;
            }
        })).values().cache();

        System.out.println("training data's num : " + traningData_Rating.count() + " validate data's num : " + validateData_Rating.count() + " test data's num : " + testData_Rating.count());

        // 为训练设置参数 每种参数设置2个值，三层for循环，一共进行8次训练
        List<Integer> ranks = new ArrayList<Integer>();
        ranks.add(8);
        ranks.add(22);

        List<Double> lambdas = new ArrayList<Double>();
        lambdas.add(0.1);
        lambdas.add(10.0);

        List<Integer> iters = new ArrayList<Integer>();
        iters.add(5);
        iters.add(7);

        // 初始化最好的模型参数
        MatrixFactorizationModel bestModel = null;
        double bestValidateRnse = Double.MAX_VALUE;
        int bestRank = 0;
        double bestLambda = -1.0;
        int bestIter = -1;

        for (int i = 0; i < ranks.size(); i++) {
            for (int j = 0; j < lambdas.size(); j++) {
                for (int k = 0; k < iters.size(); k++) {
                    //训练获得模型
                    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(traningData_Rating), ranks.get(i), iters.get(i), lambdas.get(i));
                    //通过校验集validateData_Rating获取方差，以便查看此模型的好坏，方差方法定义在最下面
                    double validateRnse = variance(model, validateData_Rating, validateData_Rating.count());
                    System.out.println("validation = " + validateRnse + " for the model trained with rank = " + ranks.get(i) + " lambda = " + lambdas.get(i) + " and numIter" + iters.get(i));

                    //将最好的模型训练结果所设置的参数进行保存
                    if (validateRnse < bestValidateRnse) {
                        bestModel = model;
                        bestValidateRnse = validateRnse;
                        bestRank = ranks.get(i);
                        bestLambda = lambdas.get(i);
                        bestIter = iters.get(i);
                    }
                }
            }
        }

        //8次训练后获取最好的模型，根据最好的模型及训练集testData_Rating来获取此方差
        double testDataRnse = variance(bestModel, testData_Rating, testData_Rating.count());
        System.out.println("the best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
                + " and numIter = " + bestIter + " and Rnse on the test data is " + testDataRnse);

        // 获取测试数据中，分数的平均值
        final double meanRating = traningData_Rating.union(validateData_Rating).mapToDouble(new DoubleFunction<Rating>() {

            @Override
            public double call(Rating t) throws Exception {
                return t.rating();
            }
        }).mean();

        // 根据平均值来计算旧的方差值
        double baseLineRnse = Math.sqrt(testData_Rating.mapToDouble(new DoubleFunction<Rating>() {

            @Override
            public double call(Rating t) throws Exception {
                return (meanRating - t.rating()) * (meanRating - t.rating());
            }
        }).mean());

        // 通过模型，数据的拟合度提升了多少
        double improvent = (baseLineRnse - testDataRnse) / baseLineRnse * 100;
        System.out.println("the best model improves the baseline by " + improvent + "%");

        //加载电影数据
        String moviepath = "file:///E:/test/MovieLens/ml-1m/movies.dat";
        JavaRDD<String> moviedata = sc.textFile(moviepath);

        // 将电影的id，标题，类型以三元组的形式保存
        JavaRDD<Tuple3<Integer, String, String>> movieList_Tuple = moviedata.map(new Function<String, Tuple3<Integer, String, String>>() {

            @Override
            public Tuple3<Integer, String, String> call(String line) throws Exception {
                String[] fields = line.split("::");
                if (fields.length != 3) {
                    throw new IllegalArgumentException("Each line must contain 3 fields");
                }
                int id = Integer.parseInt(fields[0]);
                String title = fields[1];
                String type = fields[2];
                return new Tuple3<Integer, String, String>(id, title, type);
            }
        });

        // 将电影的id，标题以二元组的形式保存
        JavaRDD<Tuple2<Integer, String>> movies_Map = movieList_Tuple.map(new Function<Tuple3<Integer,String,String>, Tuple2<Integer,String>>() {

            @Override
            public Tuple2<Integer, String> call(Tuple3<Integer, String, String> v1) throws Exception {
                return new Tuple2<Integer, String>(v1._1(), v1._2());
            }
        });

        System.out.println("movies recommond for you:");

        // 获取我所看过的电影ids
        final List<Integer> movieIds = myRatedData_Rating.map(new Function<Rating, Integer>() {

            @Override
            public Integer call(Rating v1) throws Exception {
                return v1.product();
            }
        }).collect();

        // 从电影数据中去除我看过的电影数据
        JavaRDD<Tuple2<Integer, String>> movieIdList  = movies_Map.filter(new Function<Tuple2<Integer,String>, Boolean>() {

            @Override
            public Boolean call(Tuple2<Integer, String> v1) throws Exception {
                return !movieIds.contains(v1._1);
            }
        });

        // 封装rating的参数形式，user为0，product为电影id进行封装
        JavaPairRDD<Integer, Integer> recommondList = JavaPairRDD.fromJavaRDD(movieIdList.map(new Function<Tuple2<Integer,String>, Tuple2<Integer,Integer>>() {

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, String> v1) throws Exception {
                return new Tuple2<Integer, Integer>(0, v1._1);
            }
        }));

        //通过模型预测出user为0的各product(电影id)的评分，并按照评分进行排序，获取前10个电影id
        final List<Integer> list = bestModel.predict(recommondList).sortBy(new Function<Rating, Double>() {

            @Override
            public Double call(Rating v1) throws Exception {
                return v1.rating();
            }
        }, false, 1).map(new Function<Rating, Integer>() {

            @Override
            public Integer call(Rating v1) throws Exception {
                return v1.product();
            }
        }).take(10);

        if (list != null && !list.isEmpty()) {
            //从电影数据中过滤出这10部电影，遍历打印
            movieList_Tuple.filter(new Function<Tuple3<Integer,String,String>, Boolean>() {

                @Override
                public Boolean call(Tuple3<Integer, String, String> v1) throws Exception {
                    return list.contains(v1._1());
                }
            }).foreach(new VoidFunction<Tuple3<Integer,String,String>>() {

                @Override
                public void call(Tuple3<Integer, String, String> t) throws Exception {
                    System.out.println("nmovie name --> " + t._2() + " nmovie type --> " + t._3());
                }
            });
        }

    }

    //方差的计算方法
    public static double variance(MatrixFactorizationModel model, JavaRDD<Rating> predictionData, long n) {
        //将predictionData转化成二元组型式，以便训练使用
        JavaRDD<Tuple2<Object, Object>> userProducts = predictionData.map(new Function<Rating, Tuple2<Object, Object>>() {

            public Tuple2<Object, Object> call(Rating r) {
                return new Tuple2<Object, Object>(r.user(), r.product());
            }
        });

        //通过模型对数据进行预测
        JavaPairRDD<Tuple2<Integer, Integer>, Double> prediction = JavaPairRDD.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {

            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                //System.out.println(r.user()+"..."+r.product()+"..."+r.rating());
                return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
            }
        }));

        //预测值和原值内连接
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(predictionData.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {

            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                //System.out.println(r.user() + "..." + r.product() + "..." + r.rating());
                return new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
            }
        })).join(prediction).values();

        //计算方差并返回结果
        Double dVar = ratesAndPreds.map(new Function<Tuple2<Double,Double>, Double>() {

            @Override
            public Double call(Tuple2<Double, Double> v1) throws Exception {
                return (v1._1 - v1._2) * (v1._1 - v1._2);
            }
        }).reduce(new Function2<Double, Double, Double>() {

            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });

        return Math.sqrt(dVar / n);
    }

}
