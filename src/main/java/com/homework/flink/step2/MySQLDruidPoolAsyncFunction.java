package com.homework.flink.step2;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class MySQLDruidPoolAsyncFunction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("doit01", 8888);

        int capacity = 20;

        SingleOutputStreamOperator<String> result = AsyncDataStream.orderedWait(
                lines,
                new AsyncFromMysqlDruidPool(capacity),
                1,
                TimeUnit.MINUTES,
                capacity
        );

        result.print();

        env.execute("MySQLDruidPoolAsyncFunction");

    }
}
