package com.gmg.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author gmg
 * @title: SparkHive
 * @projectName bigdata-Learning
 * @description: Java实现将Hive运算结果保存到数据库
 * @date 2019/8/6 16:07
 */
public class SparkHive {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[2]")
                .appName("SparkHive")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/").enableHiveSupport()
                .getOrCreate();

        //spark.sql.warehouse.dir为hive的hive.metastore.warehouse.dir路径
        spark.sql("show databases").show();
        spark.sql("show tables").show();
        spark.sql("use db_hive_edu");
        Dataset<Row> data = spark
                .sql("select hc_storetypeid as typeid,count(hc_storetypeid) as kczs from db_hive_edu.hc_casewoodlist where hc_wpstate=2 and hc_storetypeid !='null' group by hc_storetypeid order by hc_storetypeid");
        data.show();

        //数据库内容
        String url = "jdbc:postgresql://192.168.174.200:5432/postgres?charSet=utf-8";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","postgres");
        connectionProperties.put("password","postgres");
        connectionProperties.put("driver","org.postgresql.Driver");

        //将数据通过覆盖的形式保存在数据表中
        data.write().mode(SaveMode.Overwrite).jdbc(url, "kczyqktj", connectionProperties);

    }
}
