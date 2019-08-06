package com.gmg.spark.basic;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by hadoop on 17-4-4.
 * https://my.oschina.net/yulongblog/blog/1509506
 */
public class BasicDemo {
    public static void main(String[] args){
        //初始化
        SparkConf conf = new SparkConf().setAppName("JavaApiLearn").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        //Collection转化RDD
        List<String> list = new ArrayList<String>();
        list.add("11,22,33,44,55");
        list.add("aa,bb,cc,dd,ee");
        JavaRDD<String> jrl = jsc.parallelize(list);

        /**
         * 从文件读取转化RDD
         */
        //from hdfs to rdd
        JavaRDD<String> jrfFromHDFS = jsc.textFile("hdfs:///data/README.md");
        //from localfile to rdd
        JavaRDD<String> jrfFromLocal = jsc.textFile("file:///data/README.md");


        //PairRDD
        JavaRDD<String> jRDD = jsc.parallelize(list,1);

        JavaPairRDD<String, String> jPRDD = jRDD.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split("\\s+")[0], s.substring(s.indexOf(" ")+1));
            }
        });

        System.out.println(jPRDD.collect());


    }
}