package com.gmg.spark.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author gmg
 * @title: WordCount
 * @projectName bigdata-Learning
 * @description: https://www.jianshu.com/p/28245fb25dc9
 * @date 2019/8/6 14:45
 */
public class WordCount {
    public static void main(String[] args) {
        /**
         * 本地模式
         */
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\84407\\Desktop\\spark.txt");

        /**
         *  集群模式
         */
//        SparkConf conf = new SparkConf().setAppName("WordCount");
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> lines = sc.textFile("hdfs://s166/spark/spark.txt");


        JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> {
            //分割
            return Arrays.asList(s.split(",")).iterator();
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) word ->
                new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((Integer v1, Integer v2) -> {
            return v1 + v2;
        });

        wordCounts.foreach((Tuple2<String, Integer> wordCount) -> {
            System.out.println(wordCount._1 + "appeared " + wordCount._2);
        });
    }
}
