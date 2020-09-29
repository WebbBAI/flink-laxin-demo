package com.homework.flink.step1;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;

public class MySQLConnectPool {

    private static SQLClient mySQLClient;

    private static String jdbcUrl = "jdbc:mysql://localhost:3306/practice?characterEncoding=UTF-8";
    private static String username = "root";
    private static String password = "123456";
    private static String JDBC_DRIVER  = "com.mysql.jdbc.Driver";
    private int max_pool_size;

    public MySQLConnectPool(int max_pool_size) {
        this.max_pool_size = max_pool_size;
    }

    public SQLClient getClient() {
        JsonObject mySQLClientConfig = new JsonObject()
                .put("JDBC_DRIVER ", JDBC_DRIVER )
                .put("jdbcUrl", jdbcUrl)
                .put("username", username)
                .put("password", password)
                .put("max_pool_size",max_pool_size);

        VertxOptions vo = new VertxOptions()
                //.setEventLoopPoolSize(max_pool_size / 2)
                .setWorkerPoolSize(max_pool_size);

        Vertx vertx = Vertx.vertx(vo);

        mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);

        return mySQLClient;
    }

    public void close(){
        mySQLClient.close();
    }
}
