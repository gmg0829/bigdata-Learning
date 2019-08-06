package com.gmg.spark.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author gmg
 * @title: FunctionDemo
 * @projectName bigdata
 * @description: TODO
 * @date 2019/8/6 14:14
 */
public class FunctionDemo {
    public static void main(String[] args) {

        //初始化
        SparkConf conf = new SparkConf().setAppName("JavaApiLearn").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> strLine=new ArrayList<String>();
        strLine.add("hello world");
        strLine.add("This is Spark");
        strLine.add("This is JavaRDD");
        JavaRDD<String> input=jsc.parallelize(strLine);


        /*
         * Function<T,R>
         * 接收一个输入值并返回一个输出值，用于类似map()和filter()的操作中
         * R call(T)
         */
        //过滤RDD数据集中包含result的表项，新建RDD数据集resultLines
        JavaRDD<String> resultLines=input.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String v1)throws Exception {
                        return v1.contains("result");
                    }
                }
        );

        //以下代码的功能是wordcount，其中的reduceByKey操作的Function2函数定义了遇到相同的key时，value是如何reduce的————直接将两者的value相加。
        //将文本行的单词过滤出来，并将所有的单词保存在RDD数据集words中。切分为单词，扁平化处理。见FlatMapFunction< T,R>
        JavaRDD<String> words=input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );
        //转化为键值对
        JavaPairRDD<String,Integer> counts=words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s, 1);
                    }
                }
        );

        //对每个词语进行计数
        JavaPairRDD <String,Integer> results=counts.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        ) ;


        //将文本行的单词过滤出来，并将所有的单词保存在RDD数据集words中。
        JavaRDD<String> word=input.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" "));
                    }
                }
        );

        /*
         * PairFunction<T,K,R>
         * 接收一个输入值并返回一个Tuple，用于类似mapToPair()这样的操作中,将一个元素变为一个键值对(PairRDD)
         * Tuple2<K, V> call(T)
         */
        //转化为键值对
        JavaPairRDD<String,Integer> count=words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2(s, 1);
                    }
                }
        );


    }

}
