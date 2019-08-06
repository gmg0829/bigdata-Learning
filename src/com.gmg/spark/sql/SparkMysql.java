package com.gmg.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author gmg
 * @title: SparkMysql
 * @projectName bigdata-Learning
 * @description:  spark 读取mysql数据
 * @date 2019/8/6 15:55
 */
public class SparkMysql {
    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("JavaApiLearn").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String sql = " (select u.user_name_zh, r.organ_name from user_group_organ r, user as u "
                + "where r.user_id=u.user_id limit 1,5) as user_organ";
        SQLContext sqlContext = SQLContext.getOrCreate(JavaSparkContext.toSparkContext(sc));

        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url","jdbc:mysql://192.168.7.66:3306/mydb");//数据库路径
        reader.option("dbtable",sql);//数据表名
        reader.option("driver","com.mysql.jdbc.Driver");
        reader.option("user","root");
        reader.option("password","123321");

        Dataset<Row> projectDataSourceDFFromMySQL = reader.load();
        projectDataSourceDFFromMySQL.show();

    }
}
