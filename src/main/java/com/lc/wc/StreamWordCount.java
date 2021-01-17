package com.lc.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理字符统计.
 * @description:
 * @author: lingchen
 * @date: 2021/1/17
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度.
        // env.setParallelism(4);

        // 从文件中读取数据. -> 有界流.
        // String inputPath = "src/main/resources/wordcount.txt";
        // DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用Parameter Tool来提取host和端口.
        // 在启动参数中设置: --host localhost --port 7777.
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流读取数据. -> 无界流.
        // % nc -lk 7777
        // DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        // 基于数据流进行转换计算.
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        // 此操作并没有执行任务.
        resultStream.print();

        // 执行任务.
        env.execute();

    }
}
