package com.lc.api.windows;

import com.lc.api.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 事件时间.
 * @description:
 * @author: lingchen
 * @date: 2021/1/26
 */
public class Windows4_EventTime {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度.
        env.setParallelism(1);

        // 设置事件时间.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读入数据.
        // 无界流,socket模拟.
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换数据.
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 打印结果.
        dataStream.print("eventTime");

        // 执行.
        env.execute();
    }
}
