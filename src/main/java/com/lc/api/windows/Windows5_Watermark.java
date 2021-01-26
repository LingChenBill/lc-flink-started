package com.lc.api.windows;

import com.lc.api.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 水位线watermark设置.
 * @description:
 * @author: lingchen
 * @date: 2021/1/26
 */
public class Windows5_Watermark {

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

                // 正常升序数据设置时间戳和watermark(AscendingTimestampExtractor).
                // .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                //     @Override
                //     public long extractAscendingTimestamp(SensorReading element) {
                //         return element.getTimestamp() * 1000L;
                //     }
                // });

        // 基于事件时间的开窗聚合,统计15秒内温度的最小值.
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .minBy("temperature");

        // 打印结果(时间戳前闭后开).
        minTempStream.print("watermark");

        // nc -lk 7777
        // sensor_1,1547718199,35.8
        // sensor_2,1547718201,15.4
        // sensor_3,1547718202,6.7
        // sensor_4,1547718205,38.1
        // sensor_1,1547718207,36.3
        // sensor_1,1547718209,36.5
        // sensor_1,1547718210,34.7
        // sensor_1,1547718211,32.8
        // sensor_1,1547718212,37.1
        // sensor_1,1547718213,33
        // sensor_1,1547718224,32.1
        // sensor_1,1547718225,31.6
        // sensor_1,1547718216,21.2
        // sensor_1,1547718227,33.6
        // sensor_1,1547718240,13.3
        // sensor_1,1547718242,32.1

        // 输出:
        // watermark> SensorReading{id='sensor_1', timestamp=1547718199, temperature=35.8}
        // watermark> SensorReading{id='sensor_2', timestamp=1547718201, temperature=15.4}
        // watermark> SensorReading{id='sensor_4', timestamp=1547718205, temperature=38.1}
        // watermark> SensorReading{id='sensor_3', timestamp=1547718202, temperature=6.7}
        // watermark> SensorReading{id='sensor_1', timestamp=1547718216, temperature=21.2}
        // watermark> SensorReading{id='sensor_1', timestamp=1547718225, temperature=31.6}

        // 执行.
        env.execute();
    }
}
