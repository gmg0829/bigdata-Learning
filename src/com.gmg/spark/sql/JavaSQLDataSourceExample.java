package com.gmg.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author gmg
 * @title: JavaSQLDataSourceExample
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2020/1/20 16:01
 */
public class JavaSQLDataSourceExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
        usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");

        Dataset<Row> peopleDF =
                spark.read().format("json").load("examples/src/main/resources/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("namesAndAges.parquet");
        peopleDF
                .write()
                .partitionBy("favorite_color")
                .bucketBy(42, "name")
                .saveAsTable("people_partitioned_bucketed");



        Dataset<Row> peopleDFCsv = spark.read().format("csv")
                .option("sep", ";")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("examples/src/main/resources/people.csv");

        Dataset<Row> sqlDF =
                spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`");
        peopleDF.write().bucketBy(42, "name").sortBy("age").
                saveAsTable("people_bucketed");


        Dataset<Row> people = spark.read().json("examples/src/main/resources/people.json");
        people.printSchema();
        people.createOrReplaceTempView("people");
        Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
        namesDF.show();



        // jdbc
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .load();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "username");
        connectionProperties.put("password", "password");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

        // Saving data to a JDBC source
        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .save();

        jdbcDF2.write()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);

        // Specifying create table column data types on write
        jdbcDF.write()
                .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
        // $example off:jdbc_dataset$
    }
}
