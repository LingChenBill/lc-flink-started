package com.lc.api.transform;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 滚动算子.
 * @description:
 * @author: lingchen
 * @date: 2021/1/22
 */
public class Transform2_RollingAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成SensorReading类型.
        // DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
        //     // 转换类型.
        //     public SensorReading map(String value) throws Exception {
        //         String[] fields = value.split(",");
        //         return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        //     }
        // });

        // 使用lambda表达式(类型是否能够自动推断出).
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组.
        // Specifying keys via field positions is only valid for tuple data types.
        // KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy(0);
        // key -> pojo中的项目key.
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // lambda.
        KeyedStream<SensorReading, String> keyedStream2 = dataStream.keyBy(data -> data.getId());
        KeyedStream<SensorReading, String> keyedStream3 = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合.
        // 按key:temperature聚合,其余数据为元数据.
        DataStream<SensorReading> resultStream = keyedStream.max("temperature");
        // maxBy:key只为求最大值.显示自己的数据.
        // DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        // 求和.
        // DataStream<SensorReading> resultStream = keyedStream.sum("temperature");

        // 打印数据.
        resultStream.print();

        // 执行.
        env.execute();

    }
}
