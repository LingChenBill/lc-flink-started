package com.lc.api.transform;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Reduce算子(两个key item比较).
 * @description:
 * @author: lingchen
 * @date: 2021/1/23
 */
public class Transform3_Reduce {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成SensorReading类型.
        // 使用lambda表达式(类型是否能够自动推断出).
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组.
        // key -> pojo中的项目key.
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // redue聚合,取最大的温度值,以及当前最新的时间戳.
        // DataStream<SensorReading> resultStream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
        //     @Override
        //     public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
        //         return new SensorReading(value1.getId(), value2.getTimestamp(),
        //                 Math.max(value1.getTemperature(), value2.getTemperature()));
        //     }
        // });

        // lambda形式.
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.reduce((curState, newData) -> {
            return new SensorReading(curState.getId(), newData.getTimestamp(),
                    Math.max(curState.getTemperature(), newData.getTemperature()));
        });

        // 打印数据.
        resultStream.print();

        // 执行.
        env.execute();

    }
}
