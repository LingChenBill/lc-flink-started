package com.lc.api.source;

import com.lc.api.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * 自定义数据.
 * @description:
 * @author: lingchen
 * @date: 2021/1/21
 */
public class Source4_UDF {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度.
        env.setParallelism(1);

        // 自定义数据.
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        // 打印数据.
        dataStream.print();

        // 执行.
        env.execute();

    }

    /**
     * 实现自定义的SourceFunction.
     */
    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 定义一个标识位,用来控制数据的产生.
        private boolean running = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {

            // 定义一个随机数发生器.
            Random random = new Random();

            // 设置10个传感器的初始值.
            HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();

            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    // 在当前温度基础上随机波动.
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }

                // 控制输出频率.
                Thread.sleep(1000l);
            }

        }

        public void cancel() {
            running = false;
        }
    }
}
