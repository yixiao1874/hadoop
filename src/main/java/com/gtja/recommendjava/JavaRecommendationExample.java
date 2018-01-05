package com.gtja.recommendjava;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import scala.Tuple2;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class JavaRecommendationExample {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        SparkSession spark = SparkSession
                .builder()
                .appName("SparkPostgresqlJdbc")
                .config("spark.some.config.option","some-value")
                .getOrCreate();

        Properties connectionProperties = new Properties();

        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
        System.out.println("增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)");
        connectionProperties.put("user","root");
        connectionProperties.put("password","1874");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        /*SQLContext sqlContext = new SQLContext(jsc);
        Properties properties = new Properties();
        properties.put("user","root");
        properties.put("password","Passw0rd");
        String url = "jdbc:mysql://10.189.80.86:3306/zntg?useUnicode=true&characterEncoding=utf-8";
        String driver = "com.mysql.jdbc.Driver";*/

        /*SQLContext sqlContext = new SQLContext(jsc);
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://10.189.80.86:3306/zntg?useUnicode=true&characterEncoding=utf-8");//数据库路径
        reader.option("dbtable","dtspark");//数据表名
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","Passw0rd");*/




        // Load and parse the data
        String path = "file:///E:/test/test.dat";
        path = "file:///E:/test/test.dat";
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
        List<Rating> lists =  ratings.collect();
        for(Rating l:lists){
            System.out.println(l);
        }

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


        model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
                "target/tmp/myCollaborativeFilter");

        //使用模型为用户推荐内容
        Rating[] recommendations =sameModel.recommendProducts(1, 3);
        for(int i=0;i<recommendations.length;i++){
            System.out.println("推荐的产品:"+recommendations[i].product());
            System.out.println("被推荐用户:"+recommendations[i].user());
            System.out.println("产品打分:"+recommendations[i].rating());

            /*//SparkJdbc读取Postgresql的products表内容
            System.out.println("SparkJdbc读取Postgresql的products表内容");
            Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:postgresql://localhost:5432/postgres","products",connectionProperties).select("name","price");
            //显示jdbcDF数据内容
            jdbcDF.show();
            //将jdbcDF数据新建并写入newproducts,append模式是连接模式，默认的是"error"模式。
            jdbcDF.write().mode("append")
                    .jdbc("jdbc:postgresql://localhost:5432/postgres","newproducts",connectionProperties);*/

            // Create an instance of a Bean class
            Recommend person = new Recommend();
            person.setUserID(1);
            person.setItemID(recommendations[i].product());
            person.setPref(recommendations[i].rating());
            Encoder<Recommend> personEncoder = Encoders.bean(Recommend.class);
            Dataset<Recommend> javaBeanDS = spark.createDataset(
                    Collections.singletonList(person),
                    personEncoder
            );
            javaBeanDS.write().mode("append").jdbc("jdbc:mysql://localhost:3306/test","recommend",connectionProperties);
        }

    }

    public static class Recommend implements Serializable {
        private int userID;
        private int itemID;
        private double pref;

        public int getUserID() {
            return userID;
        }

        public void setUserID(int userID) {
            this.userID = userID;
        }

        public int getItemID() {
            return itemID;
        }

        public void setItemID(int itemID) {
            this.itemID = itemID;
        }

        public double getPref() {
            return pref;
        }

        public void setPref(double pref) {
            this.pref = pref;
        }
    }

}
