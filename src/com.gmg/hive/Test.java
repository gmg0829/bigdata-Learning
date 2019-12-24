package com.gmg.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author gmg
 * @title: Test
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/12/24 16:21
 */
public class Test {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.0.207:10000/default";
    private static String user = "hive";
    private static String password = "hive";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url,user,password);
            stmt = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建表
    public static void createTable() throws Exception {
        String sql = "create table pokes (foo int, bar string)";
        stmt.execute(sql);
        Test.destory();
    }

    // 查询所有表
    public static void showTables() throws Exception {
        String sql = "show tables";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        Test.destory();

    }

    public static void descTable() throws Exception {
        String sql = "desc emp";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1) + "\t" + rs.getString(2));
        }
        Test.destory();
    }

    public static void loadData() throws Exception {
        String filePath = "/opt/hive-2.33/examples/files/kv1.txt";
        String sql = "load data local inpath '" + filePath + "' overwrite into table pokes";
        stmt.execute(sql);
        Test.destory();

    }

    public static void selectData() throws Exception {
        String sql = "select * from pokes";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString("foo") + "\t\t" + rs.getString("bar"));
        }
        Test.destory();

    }

    public static void countData() throws Exception {
        String sql = "select count(1) from pokes";
        rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getInt(1) );
        }
        Test.destory();

    }

    public static void dropTable() throws Exception {
        String sql = "drop table if exists pokes";
        stmt.execute(sql);
        Test.destory();

    }

    public static void destory() throws Exception {
        if ( rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
