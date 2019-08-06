package com.gmg.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;

import java.util.Collections;

import static org.apache.spark.sql.functions.col;

/**
 * @author gmg
 * @title: SparkSql
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/6 14:59
 */
public class SparkSqlBasic {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        /**
         * 使用SparkSession，应用程序可以从现有RDD，Hive表或Spark数据源创建DataFrame。
         */
        Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");
        df.show();

        /**
         * 数据操作
         *
         */
        // 打印元数据
        df.printSchema();
        df.select("name").show();
        //查找name age列，age列加一
        df.select(col("name"), col("age").plus(1)).show();
        //查找age大于21的数据
        df.filter(col("age").gt(21)).show();
        //（分组查询：列名age数量统计）
        df.groupBy("age").count().show();

        //

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        //全局临时视图
        try {
            df.createGlobalTempView("people");
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        // Global temporary view is tied to a system preserved database `global_temp`（查询名字为people的全局临时视图）
        spark.sql("SELECT * FROM global_temp.people").show();

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();

        //创建数据集
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans（对javabean进行编码）
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();




    }
}
