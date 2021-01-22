package com.lc.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从Kafka中读取数据.
 * @description:
 * @author: lingchen
 * @date: 2021/1/21
 */
public class Source3_Kafka {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度.
        env.setParallelism(1);

        // 从Kafka中读取数据.
        Properties properties = new Properties();
        // properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", "hdss7-71:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>(
                "sensor", new SimpleStringSchema(), properties));

        // 打印数据.
        dataStream.print();

        // 执行.
        env.execute();

    }
}
