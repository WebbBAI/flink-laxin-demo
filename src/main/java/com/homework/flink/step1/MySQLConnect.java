package com.homework.flink.step1;

import com.google.gson.JsonObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLConnect {

    private transient static java.sql.Connection connection = null;

    private static String jdbcUrl = "jdbc:mysql://192.168.28.2:3306/bai?useSSL=false&allowPublicKeyRetrieval=true";
    private static String username = "root";
    private static String password = "123456";
    private static String driverName = "com.mysql.jdbc.Driver";

    static {
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public MySQLConnect() {
    }

    public static Connection getConnection() throws Exception{
        return connection;
    }
}
