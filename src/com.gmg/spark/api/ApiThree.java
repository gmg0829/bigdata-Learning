package com.gmg.spark.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author gmg
 * @title: ApiThree
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2020/1/21 16:10
 */
public class ApiThree {
    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("JavaApiLearn").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<User> userJavaRDD = sc.parallelize(Arrays.asList(new User("u1", 20)));
        JavaPairRDD<String, User> userJavaPairRDD = userJavaRDD.keyBy(new Function<User, String>() {
            @Override
            public String call(User user) throws Exception {
                return user.getUserId();
            }
        });
        //结果：[(u1,User{userId='u1', amount=20})]
        System.out.println("userJavaPairRDD = " + userJavaPairRDD.collect());

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 1, 2, 3, 5, 8, 13));
        Function<Integer, Boolean> isEven = new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer x) throws Exception {
                return x % 2 == 0;
            }
        };
        //将偶数和奇数分组，生成key-value类型的RDD
        JavaPairRDD<Boolean, Iterable<Integer>> oddsAndEvens = rdd.groupBy(isEven);
        //结果：[(false,[1, 1, 3, 5, 13]), (true,[2, 8])]
        System.out.println("oddsAndEvens = " + oddsAndEvens.collect());
        //结果：1
        System.out.println("oddsAndEvens.partitions.size = " + oddsAndEvens.partitions().size());

        oddsAndEvens = rdd.groupBy(isEven, 2);
        //结果：[(false,[1, 1, 3, 5, 13]), (true,[2, 8])]
        System.out.println("oddsAndEvens = " + oddsAndEvens.collect());
        //结果：2
        System.out.println("oddsAndEvens.partitions.size = " + oddsAndEvens.partitions().size());


        JavaPairRDD<String, Integer> javaPairRDD =
                sc.parallelizePairs(Arrays.asList(new Tuple2("coffee", 1), new Tuple2("coffee", 2),
                        new Tuple2("panda", 3), new Tuple2("coffee", 9)), 2);

        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = javaPairRDD.groupByKey();
        //结果：[(coffee,[1, 2, 9]), (panda,[3])]
        System.out.println("groupByKeyRDD = " + groupByKeyRDD.collect());





    }
}
