package com.lc.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 基本简单转换算子.
 * @description:
 * @author: lingchen
 * @date: 2021/1/22
 */
public class Transform1_Base {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // map.把String转换成长度输出.
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {

            public Integer map(String value) throws Exception {
                // 返回字符长度.
                return value.length();
            }
        });

        // flatmap.按逗号分字段.
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        });

        // filter.筛选sensor_1开头的id对应的数据.
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>(){

            // 筛选数据.
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        // 打印输出.
        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        // 执行.
        env.execute();

    }
}
