package com.gmg.spark.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author gmg
 * @title: ApiFour
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2020/1/21 16:49
 *  https://blog.51cto.com/7639240/1966967
 */
public class ApiFour {
    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("JavaApiLearn").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


    }
}
