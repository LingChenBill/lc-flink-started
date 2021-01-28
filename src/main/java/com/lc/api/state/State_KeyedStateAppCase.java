package com.lc.api.state;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 算子状态-键控状态应用事例.
 * @description:
 * @author: lingchen
 * @date: 2021/1/28
 */
public class State_KeyedStateAppCase {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度.
        env.setParallelism(1);

        // 读入数据.
        // 无界流,socket模拟.
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换数据.
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个flatmap操作, 检测温度跳变,输出报警.
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));          // 阈值是10.0

        // 打印.
        resultStream.print("temp-warning");

        // 输入.
        // nc -lk 7777
        // sensor_1,1547718207,36.3
        // sensor_1,1547718207,37.9
        // sensor_1,1547718209,48
        // sensor_4,1547718205,38.1
        // sensor_4,1547718205,35
        // sensor_1,1547718212,36
        // sensor_4,1547718205,46
        // sensor_1,1547718212,47

        // 输出.
        // temp-warning> (sensor_1,37.9,48.0)
        // temp-warning> (sensor_1,48.0,36.0)
        // temp-warning> (sensor_4,35.0,46.0)
        // temp-warning> (sensor_1,36.0,47.0)

        // 执行.
        env.execute();
    }

    /**
     * 自定义温度跳变报警Function.
     */
    private static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 温度跳变阈值.
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        // 定义状态,保存上一次的温度值.
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        /**
         * 转换函数.
         * @param value
         * @param out
         * @throws Exception
         */
        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            // 获取上次的温度值.
            Double lastTemp = lastTempState.value();

            // 若状态不为NULL,就判断两次温度差值.
            if (lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);

                // 若大于阈值.
                if (diff >= threshold) {
                    // 输出结果.
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
            }

            // 更新状态.
            lastTempState.update(value.getTemperature());

        }

        @Override
        public void close() throws Exception {
            // 清理状态值.
            lastTempState.clear();
        }
    }
}
