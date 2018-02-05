package com.gtja.recommendjava;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

public class BinaryMetrics {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Java Recommend");
        //.setJars(strings);
        SparkContext sc = new SparkContext(conf);

        String path = "data/mllib/sample_binary_classification_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

// Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint>[] splits =
                data.randomSplit(new double[]{0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];

// Run training algorithm to build the model.
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(2)
                .run(training.rdd());

// Clear the prediction threshold so the model will return probabilities
        model.clearThreshold();

// Compute raw scores on the test set.
        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
                new Tuple2<>(model.predict(p.features()), p.label()));

// Get evaluation metrics.
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(predictionAndLabels.rdd());

// Precision by threshold
        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
        System.out.println("Precision by threshold: " + precision.collect());

// Recall by threshold
        JavaRDD<?> recall = metrics.recallByThreshold().toJavaRDD();
        System.out.println("Recall by threshold: " + recall.collect());

// F Score by threshold
        JavaRDD<?> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
        System.out.println("F1 Score by threshold: " + f1Score.collect());

        JavaRDD<?> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
        System.out.println("F2 Score by threshold: " + f2Score.collect());

// Precision-recall curve
        JavaRDD<?> prc = metrics.pr().toJavaRDD();
        System.out.println("Precision-recall curve: " + prc.collect());

// Thresholds
        JavaRDD<Double> thresholds = precision.map(t -> Double.parseDouble(t._1().toString()));

// ROC Curve
        JavaRDD<?> roc = metrics.roc().toJavaRDD();
        System.out.println("ROC curve: " + roc.collect());

// AUPRC
        System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());

// AUROC
        System.out.println("Area under ROC = " + metrics.areaUnderROC());

// Save and load model
        /*model.save(sc, "target/tmp/LogisticRegressionModel");
        LogisticRegressionModel.load(sc, "target/tmp/LogisticRegressionModel");*/
    }
}
