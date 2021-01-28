package com.lc.api.state;

import com.lc.api.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * 算子状态.
 * @description:
 * @author: lingchen
 * @date: 2021/1/28
 */
public class State_OperatorState {

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
        SingleOutputStreamOperator<Integer> countStream = dataStream.map(new MyCountMapper());

        // 打印.
        countStream.print("count");

        // 执行.
        env.execute();
    }

    /**
     * 自定义MapFunction.
     * 并实现检查点.及时恢复.
     */
    private static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {

        // 定义一个本地变量,作为算子状态.
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        /**
         * 对算子状态进行快照.
         * @param checkpointId
         * @param timestamp
         * @return
         * @throws Exception
         */
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        /**
         * 恢复算子状态.
         * @param state
         * @throws Exception
         */
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
