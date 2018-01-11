/*
 * ================================================================
 * Copyright 2008-2015 AMT.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * AMT Corp. Ltd, ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with AMT.
 * 
 * 国泰君安智能投顾项目	
 *
 * ================================================================
 *  创建人: lipeipei
 *	创建时间: 2018年1月10日 - 上午8:54:55
 */
package com.gtja.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * <p>
 * @TODO 请“lipeipei” 尽快添加代码注释!（中文表达，简要说明）
 * </p>
 *
 * @author lipeipei
 *
 * @version 1.0.0
 *
 * @since 1.0.0
 *
 */
public class SparkFile {

    public static void main(String args[]) throws IOException {
    	
    	/*String path1 = new File(".").getCanonicalPath();
    	System.getProperties().put("hadoop.home.dir", path1);
   		new File("./bin").mkdirs();
    	new File("./bin/winutils.exe").createNewFile();*/
    	
        // $example on$
        SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = "file:///E:/test/source.csv";
        JavaRDD<String> data = jsc.textFile(path);

        SQLContext sqlContext = new SQLContext(jsc);
        String url = "jdbc:mysql://192.168.56.1:3306/test?useSSL=false&autoReconnect=true&failOverReadOnly=false";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","1874");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");
        
        JavaRDD<CustAndStkInfo> custRDD = data.map(
                new Function<String, CustAndStkInfo>() {
                    public CustAndStkInfo call(String s) {
                        String[] sarray = s.split(",");
                        CustAndStkInfo custAndStkInfo = new CustAndStkInfo(sarray[0],sarray[1],new BigDecimal(sarray[2]),Integer.parseInt(sarray[3]),
                        	Integer.parseInt(sarray[4]),Integer.parseInt(sarray[5]),new BigDecimal(sarray[6]),Integer.parseInt(sarray[7]),
                        	Integer.parseInt(sarray[8]));
                        return custAndStkInfo;
                    }
                }
        );

        CustAndStkInfo custAndStkInfo = custRDD.first();
        System.out.println(custAndStkInfo.getCustomer_no());

        JavaRDD<SimpleRating> ratRDD = custRDD.map(
                new Function<CustAndStkInfo, SimpleRating>() {
                    public SimpleRating call(CustAndStkInfo custAndStkInfo) {
                        double hold_stock_ratio = holdStockRatio(custAndStkInfo.getHold_asset_avg().doubleValue(), custAndStkInfo.getHold_asset().doubleValue());
                        double hold_days_ratio = holdDaysRatio(custAndStkInfo.getHold_days(), custAndStkInfo.getTotal_hold_days());
                        double buy_times_ratio = buyTimesRatio(custAndStkInfo.getMatch_cnt(), custAndStkInfo.getTotal_match_cnt());
                        double buy_lastdate_ratio = buyLastdateRatio(custAndStkInfo.getIn_match_lastdate(), new Date());
                        double score = compileScore(hold_stock_ratio, hold_days_ratio, buy_times_ratio, buy_lastdate_ratio);
                        SimpleRating simpleRating = new SimpleRating("4234321", "321412", score);
                        return simpleRating;
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
                            } catch (Exception e) {
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


        JavaRDD<Row> rowRDD = ratRDD.map(
                new Function<SimpleRating, Row>() {
                    public Row call(SimpleRating rating) {
                        return RowFactory.create(rating.getCustomer_no(),rating.getStock_code(),rating.getScore());
                    }
                }
        );



        /**
         * 第二步：动态构造DataFrame的元数据。
         */
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("userID",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("itemID",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("pref",DataTypes.DoubleType,true));


        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType = DataTypes.createStructType(structFields);

        /**
         * 第三步：基于已有的元数据以及RDD<Row>来构造DataFrame
         */
        Dataset<Row> scoreDS = sqlContext.createDataFrame(rowRDD,structType);


        /**
         * 第四步：将数据写入到person表中
         */
        scoreDS.write().mode("append").jdbc(url,"recommend",connectionProperties);



        //停止SparkContext
        jsc.stop();
    }
}
