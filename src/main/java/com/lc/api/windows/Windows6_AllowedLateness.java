package com.lc.api.windows;

import com.lc.api.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * 水位线watermark设置.
 * 迟到数据处理.
 * @description:
 * @author: lingchen
 * @date: 2021/1/26
 */
public class Windows6_AllowedLateness {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度.
        env.setParallelism(1);

        // 设置事件时间.
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置watermark 时间间隔.
        env.getConfig().setAutoWatermarkInterval(100l);

        // 读入数据.
        // 无界流,socket模拟.
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换数据.
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                // 乱序数据设置时间戳和watermark(BoundedOutOfOrdernessTimestampExtractor方式).
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        // 毫秒数.
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 迟到数据的tag.
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {};

        // 基于事件时间的开窗聚合,统计15秒内温度的最小值.推迟2秒.
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))           // 迟到时间设置为1秒.
                .sideOutputLateData(outputTag)              // 设置延迟数据tag.
                .minBy("temperature");

        // 打印结果(时间戳前闭后开).
        minTempStream.print("watermark");

        // 打印迟到数据.
        minTempStream.getSideOutput(outputTag).print("late");

        // nc -lk 7777
        // sensor_1,1547718199,35.8
        // sensor_1,1547718210,34.7
        // sensor_1,1547718211,32.8
        // sensor_1,1547718212,37.1
        // sensor_1,1547718225,31.6
        // sensor_1,1547718227,33.6
        // sensor_1,1547718270,31
        // sensor_1,1547718203,31.9
        // sensor_1,1547718272,34
        // sensor_1,1547718203,30.6
        // sensor_1,1547718203,31.9

        // 输出:
        // watermark> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
        // watermark> SensorReading{id='sensor_1', timestamp=1547718211, temperature=32.8}
        // watermark> SensorReading{id='sensor_1', timestamp=1547718225, temperature=31.6}
        // watermark> SensorReading{id='sensor_1', timestamp=1547718203, temperature=31.9}
        // late> SensorReading{id='sensor_1', timestamp=1547718203, temperature=30.6}
        // late> SensorReading{id='sensor_1', timestamp=1547718203, temperature=31.9}

        // 执行.
        env.execute();
    }
}
