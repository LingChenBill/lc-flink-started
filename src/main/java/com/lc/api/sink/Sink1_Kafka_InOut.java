package com.lc.api.sink;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * sink与kafka连接, 输入与输出.
 * @description:
 * @author: lingchen
 * @date: 2021/1/23
 */
public class Sink1_Kafka_InOut {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从Kafka中读取数据.
        Properties properties = new Properties();
        // properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", "hdss7-71:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>(
                "sensor", new SimpleStringSchema(), properties));

        // 转换成SensorReading类型.
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        // kafka sink连接.
        dataStream.addSink(new FlinkKafkaProducer<String>(
                "hdss7-71:9092",
                "sink_kafka",
                new SimpleStringSchema()
        ));


        // 执行.
        env.execute();

    }
}
