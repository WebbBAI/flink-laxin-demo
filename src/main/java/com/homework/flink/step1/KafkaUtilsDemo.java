package com.homework.flink.step1;

import com.alibaba.fastjson.JSON;
import com.homework.flink.utils.KafkaUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class KafkaUtilsDemo {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        //从flink工具类中获取抽取kafka数据的Datastream
        DataStream<String> lines = KafkaUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        System.out.println(lines);

        StreamExecutionEnvironment env = KafkaUtils.getEnv();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<TestBean1> mapped = lines.map(new MapFunction<String, TestBean1>() {
            @Override
            public TestBean1 map(String line) throws Exception {
                TestBean1 testBean1 = JSON.parseObject(line, TestBean1.class);
                return testBean1;
            }
        });

        SingleOutputStreamOperator<TestBean1> waterMarksBean = mapped.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TestBean1>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(TestBean1 element) {
                return element.event_time;
            }
        });

        tableEnv.registerDataStream("test", waterMarksBean, "event_id, unionid, act_id, channel_id, event_time");

        //使用sql方式查询
        Table table = tableEnv.sqlQuery("select event_id, count(1) from test group by event_id");

        TableSchema schema = table.getSchema();
        System.out.println(schema);

        //转换成可更新的数据流
        DataStream<Tuple2<Boolean, Row>> resRest = tableEnv.toRetractStream(table, Row.class);
        resRest.print();
        /*tableEnv.toAppendStream(table, String.class);*/

        env.execute("test");
    }
}
