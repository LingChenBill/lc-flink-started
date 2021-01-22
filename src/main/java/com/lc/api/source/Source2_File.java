package com.lc.api.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据.
 * @description:
 * @author: lingchen
 * @date: 2021/1/21
 */
public class Source2_File {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度.
        env.setParallelism(1);

        // 从文件中读取数据.
        DataStream<String> dataStream = env.readTextFile("src/main/resources/sensor.txt");

        // 打印输出.
        dataStream.print();

        // 执行.
        env.execute();

    }
}
