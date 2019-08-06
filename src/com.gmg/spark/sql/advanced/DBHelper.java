package com.gmg.spark.sql.advanced;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author gmg
 * @title: DBHelper
 * @projectName bigdata-Learning
 * @description: TODO
 * @date 2019/8/6 16:19
 */
public class DBHelper {

    private String url = "jdbc:mysql://192.168.27.166:3306/nginx";
    private String name = "com.mysql.jdbc.Driver";
    private String user = "root";
    private String password = "xxx";

    //获取数据库连接
    public Connection connection = null;

    public DBHelper(){
        try {
            Class.forName(name);
            connection = DriverManager.getConnection(url,user,password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void close() throws SQLException {
        this.connection.close();
    }
}
