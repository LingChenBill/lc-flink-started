package com.lc.api.windows;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 窗口分配器: 滚动窗口,滑动窗口,会话窗口,全局窗口.滚动计数与滑动计数窗口.
 * @description:
 * @author: lingchen
 * @date: 2021/1/26
 */
public class Windows1_TimeWindows {

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

        // 开窗测试.
        DataStream<Integer> resultStream = dataStream.keyBy("id")
                // .timeWindow(Time.seconds(5));                              // TumblingProcessingTimeWindows.
                // .timeWindow(Time.seconds(2), Time.seconds(10));            // SlidingProcessingTimeWindows.
                // .window(EventTimeSessionWindows.withGap(Time.minutes(1)));    // EventTimeSessionWindows.
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));     // TumblingProcessingTimeWindows.
                // .countWindow(15, 5);
                .timeWindow(Time.seconds(5))
                // 增量聚合.
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    /**
                     * 初期值.
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    /**
                     * 增量规则.每一条数据,如何增量.
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    /**
                     * 返回结果.
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    /**
                     * 每个窗口的合并操作.
                     * @param a
                     * @param b
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // 打印结果.
        resultStream.print("aggregate");

        // 执行.
        env.execute();
    }
}
