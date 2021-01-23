package com.lc.api.transform;

import com.lc.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分区(shuffle).
 * @description:
 * @author: lingchen
 * @date: 2021/1/23
 */
public class Transform6_Partition {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 读入数据.
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // inputStream.print("input");

        // shuffle操作分区.
        DataStream<String> shuffleStream = inputStream.shuffle();

        // shuffleStream.print("shuffle");

        // keyBy.
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // dataStream.keyBy("id").print("keyBy");

        // global.
        dataStream.global().print("global");

        env.execute();

    }
}
