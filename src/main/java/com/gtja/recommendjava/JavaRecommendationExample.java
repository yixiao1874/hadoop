package com.gtja.recommendjava;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;

import java.util.*;


public class JavaRecommendationExample {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Java Recommend");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(jsc);
        /*String url = "jdbc:mysql://localhost:3306/test";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","1874");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");
*/
        String url = "jdbc:mysql://10.189.80.86:3306/zntg?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","PasswOrd");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");


        // Load and parse the data
        String path = args[0];
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

        //取出输入的数据Rating(1,101,1.0)
        //Rating(1,102,5.0)
        //Rating(1,105,2.0)
        //Rating(1,108,1.0)
        /*List<Rating> lists =  ratings.collect();
        for(Rating l:lists){
            System.out.println(l);
        }*/

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
        //删除临时目录
        /*Configuration configuration = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.56.100:9000");
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            fileSystem.delete(new Path("target/tmp/myCollaborativeFilter"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }*/


        /*model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
                "target/tmp/myCollaborativeFilter");*/


        //使用模型为用户推荐内容
        Rating[] recommendations =model.recommendProducts(1, 3);
        for(int i=0;i<recommendations.length;i++){
            System.out.println("推荐的产品为:"+recommendations[i].product());
            System.out.println("被推荐用户:"+recommendations[i].user());
            System.out.println("产品打分:"+recommendations[i].rating());

            //写入的数据内容
            JavaRDD<String> personData =
                    jsc.parallelize(Arrays.asList(recommendations[i].user()+" "+recommendations[i].product()+" "+recommendations[i].rating()));


            /**
             * 第一步：在RDD的基础上创建类型为Row的RDD
             */
            //将RDD变成以Row为类型的RDD。Row可以简单理解为Table的一行数据
            JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
                public Row call(String line) throws Exception {
                    String[] splited = line.split(" ");
                    return RowFactory.create(Integer.valueOf(splited[0]),Integer.valueOf(splited[1]),Double.valueOf(splited[2]));
                }
            });

            /**
             * 第二步：动态构造DataFrame的元数据。
             */
            List structFields = new ArrayList();
            structFields.add(DataTypes.createStructField("customer_no",DataTypes.IntegerType,true));
            structFields.add(DataTypes.createStructField("stock_code",DataTypes.IntegerType,true));
            structFields.add(DataTypes.createStructField("score",DataTypes.DoubleType,true));

            //构建StructType，用于最后DataFrame元数据的描述
            StructType structType = DataTypes.createStructType(structFields);

            /**
             * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
             */
            Dataset personsDF = sqlContext.createDataFrame(personsRDD,structType);

            /**
             * 第四步：将数据写入到person表中
             */
            personsDF.write().mode("append").jdbc(url,"CUST_SCORE_STK",connectionProperties);
        }

    }

}
