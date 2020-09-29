package com.homework.flink.step1;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class AsyncQueryFromMySQL {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> lines = env.socketTextStream("doit01", 8888);

        int capacity = 20;

//        SingleOutputStreamOperator<Row> result = AsyncDataStream.orderedWait(
//                lines,
//                new MySQLAsyncFunction(20),
//                1000,
//                TimeUnit.MICROSECONDS,
//                capacity
//        );

        SingleOutputStreamOperator<List<JsonObject>> result = lines.map(new RichMapFunction<String, List<JsonObject>>() {
            MySQLConnectPool client = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                client = new MySQLConnectPool(20);
            }

            @Override
            public List<JsonObject> map(String line) throws Exception {
                String[] fields = line.split("-");
                System.out.println("准备查询数据库。。");
                String querySQL = "select " + fields[0] + " from " + fields[1] + " where " + fields[2] + ";";
                System.out.println(querySQL);
                List<JsonObject> rows = null;
                client.getClient().query(querySQL, new Handler<AsyncResult<ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<ResultSet> event) {
                        if (event.succeeded()) {
                            List<JsonObject> rows = event.result().getRows();
                        }
                    }
                });
                System.out.println("完成数据库查询。。");
                return rows;
            }

            @Override
            public void close() throws Exception {
                client.close();
                super.close();
            }
        });

        result.print();

        env.execute("AsyncQueryFromMySQL");
    }
}
