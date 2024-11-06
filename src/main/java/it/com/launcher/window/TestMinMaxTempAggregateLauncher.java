package it.com.launcher.window;

import it.com.entity.MinMaxTemp;
import it.com.entity.SensorReading;
import it.com.function.ds.SensorSource;
import it.com.function.window.MinMaxTempAggregateFunction;
import it.com.function.window.MinMaxTempProcessWindowFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 传感器温度统计程序 - 使用AggregateFunction和ProcessWindowFunction组合实现
 * 使用5秒的滚动窗口计算每个传感器的最高温度和最低温度
 */
public class TestMinMaxTempAggregateLauncher {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建传感器数据流
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        // 使用aggregate和process组合计算最大最小温度
        DataStream<MinMaxTemp> minMaxTempPerWindow = sensorData.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new MinMaxTempAggregateFunction(), new MinMaxTempProcessWindowFunction());

        minMaxTempPerWindow.print();

        env.execute("Compute min and max temperature with aggregate");
    }
}