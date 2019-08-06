package com.gmg.spark.sql.advanced;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author gmg
 * @title: NginxLogCollect
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/6 18:40
 */
public class NginxLogCollect implements Serializable {

    static DBHelper dbHelper = null;
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("NginxLogCollect").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\84407\\Desktop\\nginx.log");
        JavaRDD<NginxParams> nginxs = lines.map((Function<String, NginxParams>) line -> {
            Pattern p = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) (\\[.*\\]) (\\\".*?\\\") (-|[0-9]*) (-|[0-9]*) (\\\".*?\\\") (\\\".*?\\\")([^ ]*)");
            Matcher m = p.matcher(line);
            NginxParams nginxParams = new NginxParams();
            while (m.find()){
                nginxParams.setRemoteAddr(m.group(1));
                nginxParams.setRemoteUser(m.group(2));
                nginxParams.setTimeLocal(m.group(4));
                nginxParams.setRequest(m.group(5));
                nginxParams.setStatus(m.group(6));
                nginxParams.setByteSent(m.group(7));
                nginxParams.setReferer(m.group(8));
                nginxParams.setHttpUserAgent(m.group(9));
                nginxParams.setHttpForwardedFor(m.group(10));
            }
            return nginxParams;
        });
        /**
         * 使用反射方式，将RDD转换为DataFrame
         */
        Dataset<Row> nginxDF = sqlContext.createDataFrame(nginxs,NginxParams.class);
        /**
         * 拿到一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行sql语句
         */
        nginxDF.registerTempTable("nginxs");

        Dataset<Row> allDF = sqlContext.sql("select * from nginxs");
        //统计ip访问数
        Dataset<Row> addrCount = sqlContext.sql("select remoteAddr,COUNT(remoteAddr)as count from nginxs GROUP BY remoteAddr  ORDER BY count DESC");
        /**
         * 将查询出来的DataFrame ，再次转换为RDD
         */
        JavaRDD<Row> allRDD = allDF.javaRDD();
        JavaRDD<Row> addrCountRDD = addrCount.javaRDD();
        /**
         * 将RDD中的数据进行映射，映射为NginxParams
         */
        JavaRDD<NginxParams> map = allRDD.map((Function<Row, NginxParams>) row -> {
            NginxParams nginxParams = new NginxParams();
            nginxParams.setRemoteAddr(row.getString(4));
            nginxParams.setRemoteUser(row.getString(5));
            nginxParams.setTimeLocal(row.getString(8));
            nginxParams.setRequest(row.getString(6));
            nginxParams.setStatus(row.getString(7));
            nginxParams.setByteSent(row.getString(0));
            nginxParams.setReferer(row.getString(2));
            nginxParams.setHttpUserAgent(row.getString(3));
            nginxParams.setHttpForwardedFor(row.getString(1));
            return nginxParams;
        });

        /**
         * 将数据collect回来，然后打印
         */

        //        List<NginxParams> nginxParamsList = map.collect();
        //        for (NginxParams np:nginxParamsList){
        //            System.out.println(np);
        //        }

        dbHelper = new DBHelper();
        String sql = "INSERT INTO `access` VALUES (?,?,?,?,?,?,?,?,?)";
        map.foreach((VoidFunction<NginxParams>) nginxParams -> {
            PreparedStatement pt = dbHelper.connection.prepareStatement(sql);
            pt.setString(1,nginxParams.getRemoteAddr());
            pt.setString(2,nginxParams.getRemoteUser());
            pt.setString(3,nginxParams.getTimeLocal());
            pt.setString(4,nginxParams.getRequest());
            pt.setString(5,nginxParams.getStatus());
            pt.setString(6,nginxParams.getByteSent());
            pt.setString(7,nginxParams.getReferer());
            pt.setString(8,nginxParams.getHttpUserAgent());
            pt.setString(9,nginxParams.getHttpForwardedFor());
            pt.executeUpdate();
        });

        String addrCountSql = "insert into `acc_addr_count` values(?,?)";
        addrCountRDD.foreach((VoidFunction<Row>) row -> {
            System.out.println("row.getString(0)"+row.getString(0));
            System.out.println("row.getString(1)"+row.getLong(1));
            PreparedStatement pt = dbHelper.connection.prepareStatement(addrCountSql);
            pt.setString(1,row.getString(0));
            pt.setString(2, String.valueOf(row.getLong(1)));
            pt.executeUpdate();
        });
    }
}