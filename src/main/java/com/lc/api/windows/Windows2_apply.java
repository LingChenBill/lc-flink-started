package com.lc.api.windows;

import com.lc.api.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 窗口分配器: 全窗口聚合.
 * @description:
 * @author: lingchen
 * @date: 2021/1/26
 */
public class Windows2_apply {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读入数据.
        // 有界流.
        // DataStreamSource<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 无界流,socket模拟.
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换数据.
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 全窗口函数测试.
        DataStream<Tuple3<String, Long, Integer>> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    /**
                     * 全窗口的获取信息比较全面.
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input,
                                      Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        // 数据id.
                        String id = tuple.getField(0);
                        // 结束时间戳.
                        Long timeEnd = window.getEnd();
                        // 输入大小.
                        int size = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, timeEnd, size));
                    }
                });

        // 打印结果.
        resultStream.print("apply");

        // 执行.
        env.execute();
    }
}
