package com.gtja.recommendjava;

import org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrix;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;


public class JavaRecommendationExample {
    public static void main(String args[]) {
        String[] strings = {"out/artifacts/hadoop_jar/hadoop.jar"};
        SparkConf conf = new SparkConf().setAppName("Java Recommend");
               //.setJars(strings);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        /*JavaRDD<String> text = jsc.textFile("/test/text");
        JavaRDD<Rating> object = jsc.objectFile("/test/object");


        System.out.println(text.first());
        System.out.println(object.first().user());


        JavaRDD<Rating> ratings = text.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0].substring(7)), Integer.parseInt(sarray[1]),
                        Double.parseDouble(sarray[2].substring(0,sarray[2].length()-1)));
                    }
                }
        );

        System.out.println(ratings.first().user());*/

        /*SQLContext sqlContext = new SQLContext(jsc);
        String url = "jdbc:mysql://192.168.56.1:3306/test?useSSL=false&autoReconnect=true&failOverReadOnly=false";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","1874");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");*/


        // Load and parse the data
        String path = args[0];

        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Rating> ratings = data.map(
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


        // Build the recommendation model using ALS
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
        //MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        //忽略评分数据进行模型训练
        MatrixFactorizationModel model = ALS.trainImplicit(JavaRDD.toRDD(ratings), rank, numIterations, lambda, 0.01);


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
        JavaRDD<Tuple2<Object,Rating[]>> recommendRDD = model.recommendProductsForUsers(10).toJavaRDD();

        /*Rating[] ratings1 = model.recommendProducts(1,10);
        for(Rating r:ratings1){
            System.out.println("为用户" + r.user() +"推荐商品" +r.product() +"喜爱度：" + r.rating());
        }*/


        /*recommendRDD.foreachPartition(new VoidFunction<java.util.Iterator<Tuple2<Object, Rating[]>>>() {
            @Override
            public void call(java.util.Iterator<Tuple2<Object, Rating[]>> tuple2Iterator) throws Exception {
                Connection conn = null;
                PreparedStatement ps = null;
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://192.168.56.1:3306/test","root","1874");
                ps = conn.prepareStatement("INSERT INTO recommend (userID,itemID,pref) VALUES (?,?,?)");
                while(tuple2Iterator.hasNext()){
                    Tuple2<Object, Rating[]> tuple2 = tuple2Iterator.next();
                    Rating[] ratings1 = tuple2._2;
                    for(Rating r:ratings1){
                        ps.setInt(1,r.user());
                        ps.setInt(2,r.product());
                        ps.setDouble(3,r.rating());
                        ps.execute();
                    }
                }
                *//*Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(
                        "jdbc:mysql://10.189.80.86:3306/zntg?characterEncoding=utf8&useSSL=false","root","******");
                ps = conn.prepareStatement("INSERT INTO RECOMMEND_RESULT (customer_no,stock_code,score) VALUES (?,?,?)");
                while(tuple2Iterator.hasNext()){
                    Tuple2<Object, Rating[]> tuple2 = tuple2Iterator.next();
                    Rating[] ratings1 = tuple2._2;
                    for(Rating r:ratings1){
                        ps.setInt(1,r.user());
                        ps.setInt(2,r.product());
                        ps.setDouble(3,r.rating());
                        ps.execute();
                    }
                }*//*
                ps.close();
                conn.close();
            }
        });*/

        /*List<Tuple2<Object, Rating[]>> tuple2 = recommendRDD.take(2);
        for(Tuple2<Object,Rating[]> t :tuple2){
            Rating[] rating = t._2;
            for(Rating r:rating){
                System.out.println("为用户"+r.user()+"推荐"+r.product()+"预测用户喜爱度"+r.rating());
            }
        }
        recommendRDD.saveAsTextFile("/test/rec1");
        recommendRDD.saveAsObjectFile("/test/rec2");*/

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
        /*model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(), "target/tmp/myCollaborativeFilter");*/
    }
}
