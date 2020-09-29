package com.homework.flink.step2;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncFromMysqlDruidPool extends RichAsyncFunction<String, String> {

    private transient ExecutorService executorService = null;
    private transient DruidDataSource dataSource = null;

    private int maxConnTotal;

    public AsyncFromMysqlDruidPool(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建一个线程池（为了实现并发请求的）
        executorService = Executors.newFixedThreadPool(maxConnTotal);
        //创建连接池（异步IO 一个请求就是一个线程，一个请求对应一个连接）
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        dataSource.setUrl("jdbc:mysql://192.168.28.3:3306/bigdata?characterEncoding=UTF-8");
        dataSource.setMaxActive(maxConnTotal);
    }

    @Override
    public void asyncInvoke(String line,final ResultFuture<String> resultFuture) throws Exception {
        //将一个查询的请求丢入查询的线程池中
        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return queryFromMySQL(line);
            }
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).thenAccept((String result) -> {
            resultFuture.complete(Collections.singleton(result));
        });
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
        super.close();
    }

    private String queryFromMySQL(String line) throws SQLException {
        String querySQL = "select group_id,act_id,act_type,create_date from dim_group_info where group_id = ?";
        String result = null;
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            stmt = connection.prepareStatement(querySQL);
            stmt.setString(1, line);
            rs = stmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("group_id") + ",";
                result += rs.getString("act_id") + ",";
                result += rs.getString("act_type") + ",";
                result += rs.getString("create_date");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return result;
    }
}
