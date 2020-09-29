package com.homework.flink.step1;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class KafkaConnect {
    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test1")
                .startFromEarliest()
                .property("bootstrap.servers", "doit01:9092,doit02:9092")
        ).withFormat(new Json().failOnMissingField(false).deriveSchema())
                .withSchema(new Schema()
                .field("event_id", TypeInformation.of(Integer.class))
                .field("unionid", TypeInformation.of(String.class))
                .field("act_id", TypeInformation.of(Integer.class))
                .field("channel_id", TypeInformation.of(Integer.class))
                .field("event_time", TypeInformation.of(Long.class))
        ).inAppendMode().registerTableSource("kafkaSource");

        Table table = tableEnv.sqlQuery("select event_id, count(1) as cnts from kafkaSource group by event_id");

        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);

        retractStream.print();

        env.execute("test2");
    }
}
