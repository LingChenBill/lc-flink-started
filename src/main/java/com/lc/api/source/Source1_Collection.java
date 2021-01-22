package com.lc.api.source;

import com.lc.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * 从集合中读取数据.
 * @description:
 * @author: lingchen
 * @date: 2021/1/21
 */
public class Source1_Collection {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度.
        env.setParallelism(1);

        // 读入数据.
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_2", 1547718201L, 15.4),
                new SensorReading("sensor_3", 1547718202L, 6.7),
                new SensorReading("sensor_4", 1547718205L, 38.1)
        ));

        DataStream<Integer> integerDataStream = env.fromElements(1, 2, 4, 67, 189);

        // 打印输出,流的名称.
        dataStream.print("data");
        integerDataStream.print("int");
        // integerDataStream.print("int").setParallelism(1);

        // 执行.
        env.execute();

    }
}
