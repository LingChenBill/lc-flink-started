package com.lc.api.transform;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

/**
 * 富文本函数.
 * @description:
 * @author: lingchen
 * @date: 2021/1/23
 */
public class Transform5_RichFunction {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度.
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成SensorReading类型.
        // 使用lambda表达式(类型是否能够自动推断出).
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 转换流.
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        // 打印数据.
        resultStream.print();

        // 执行.
        env.execute();

    }

    /**
     * MyMapper 实现MapFunction.
     */
    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<String, Integer>(value.getId(), value.getId().length());
        }
    }

    /**
     * MyMapper富函数.
     */
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<String, Integer>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化操作,一般是定义状态,或者建立外部数据库连接.
            System.out.println("RichMapFunction open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("RichMapFunction close");
        }
    }
}
