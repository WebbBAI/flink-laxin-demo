package com.homework.flink.step1;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MySQLAsyncFunction extends RichAsyncFunction<String, Row> {

    private transient MySQLConnectPool connectPool;
    private transient SQLClient mySQLClient;
    private transient ExecutorService executorService;

    private int maxConnTotal;

    public MySQLAsyncFunction(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //创建一个线程池（为了实现并发请求的）
        executorService = Executors.newFixedThreadPool(maxConnTotal);
        //创建连接池（异步IO 一个请求一个线程 一个线程对应一个链接）
        connectPool = new MySQLConnectPool(maxConnTotal);
        mySQLClient = connectPool.getClient();
    }

    @Override
    public void asyncInvoke(String line, ResultFuture<Row> resultFuture) throws Exception {
        String[] fields = line.split("-");

        mySQLClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                if (sqlConnectionAsyncResult.failed()){
                    return;
                }
                SQLConnection connection = sqlConnectionAsyncResult.result();

                String querySQL = "select " + fields[0] + " from " + fields[1] + " where" + fields[2];

                connection.query(querySQL, new Handler<AsyncResult<io.vertx.ext.sql.ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<io.vertx.ext.sql.ResultSet> resultSetAsyncResult) {
                        List<Row> rows = null;
                        if (resultSetAsyncResult.succeeded()){
                            rows = Collections.singletonList((Row) resultSetAsyncResult.result().getRows());
                        }
                        resultFuture.complete(rows);
                    }
                });
            }
        });
    }

    @Override
    public void timeout(String input, ResultFuture<Row> resultFuture) throws Exception {

    }

    @Override
    public void close() throws Exception {
        connectPool.close();
        executorService.shutdown();
        super.close();
    }
}
