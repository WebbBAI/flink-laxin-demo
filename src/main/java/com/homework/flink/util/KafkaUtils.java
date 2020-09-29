package com.homework.flink.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Kafka工具包
 */

public class KafkaUtils {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /*
        创建KafkaSource
     */
    public static<T> DataStream<T> createKafkaStream(ParameterTool parameters, Class<? extends DeserializationSchema<T>> clazz) throws Exception{

        //开启checkpoint
        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 30000));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //设置state存储的后端
        env.setStateBackend(new FsStateBackend(parameters.getRequired("checkpoint.path")));

        //从kafka中读取数据，调用kafkasource，创建Datastream
        //kafka配置文件
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
        properties.setProperty("auto.offset.reset", parameters.get("auto.offset.reset", "earliest"));
        properties.setProperty("group.id", parameters.getRequired("group.id"));

        String topics = parameters.getRequired("kafka.input.topics");

        List<String> topicList = Arrays.asList(topics.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(topicList, clazz.newInstance(), properties);

        //设置kafka不自动提交偏移量
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
