package com.lc.api.sink;

import com.lc.api.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * sink与Jdbc(mysql)连接.
 * @description:
 * @author: lingchen
 * @date: 2021/1/23
 */
public class Sink4_Jdbc {

    public static void main(String[] args) throws Exception {

        // 执行环境.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度.
        env.setParallelism(1);

        // 读入数据.
        DataStream<String> inputStream = env.readTextFile("src/main/resources/sensor.txt");

        // 转换成SensorReading类型.
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // mysql 自定义sink连接.
        dataStream.addSink(new MyJdbcSink());

        // 执行.
        env.execute();
    }

    /**
     * 自定义的Jdbc的SinkFunction.
     */
    private static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        // mysql连接器.
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        /**
         * mysql的DB连接配置.
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/book", "root", "Aa123456");
            // 插入语句.
            insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
            // 更新语句.
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        /**
         * 执行语句.
         * 每来一条数据,调用连接,执行sql.
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            // 直接执行更新语句,若没有更新那么就插入.
            // 更新操作.
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                // 插入操作.
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        /**
         * 关闭连接.
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
