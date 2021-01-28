package com.lc.api.state;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 算子状态-键控状态.
 * @description:
 * @author: lingchen
 * @date: 2021/1/28
 */
public class State_KeyedState {

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

        // 定义一个有状态的map操作,统计当前分区数据个数.
        SingleOutputStreamOperator<Integer> countStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        // 打印.
        countStream.print("key-count");

        // 执行.
        env.execute();
    }

    /**
     * 自定义RichMapFunction.
     */
    private static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        // 键控状态.
        private ValueState<Integer> keyCountState;

        // 其它类型状态的声明.
        private ListState<String> myListState;

        // map state.
        private MapState<String, Double> myMapState;

        // reduce state.
        private ReducingState<SensorReading> myReducingState;

        /**
         * 在open方法里获取ValueState.
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            // 推荐使用, 后面获取keyCountState进行判断.
            // keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));

            // list state.
            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<String>("my-list", String.class));

            // map state.
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

            // reduce state.
            // myReducingState = getRuntimeContext().getReducingState(
            //         new ReducingStateDescriptor<SensorReading>(""))
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            // 获取count.
            Integer count = keyCountState.value();
            count++;

            // 更新count.
            keyCountState.update(count);

            // 其它状态的API调用.
            // list state.
            for (String str : myListState.get()) {
                System.out.println(str);
            }
            // Iterable<String> strings = myListState.get();
            // myListState.add("Hello");

            // map state.
            // myMapState.get("1");
            // myMapState.put("2", new Double(32.3));


            return count;
        }
    }
}
