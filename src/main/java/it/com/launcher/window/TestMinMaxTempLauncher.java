package it.com.launcher.window;

import it.com.entity.MinMaxTemp;
import it.com.entity.SensorReading;
import it.com.function.ds.SensorSource;
import it.com.function.window.HighAndLowTempProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 传感器温度统计程序
 * 使用5秒的滚动窗口计算每个传感器的最高温度和最低温度
 */
public class TestMinMaxTempLauncher {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建传感器数据流
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        // 按传感器ID分组，开窗计算最大最小温度
        DataStream<MinMaxTemp> minMaxTempPerWindow = sensorData.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new HighAndLowTempProcessFunction());

        minMaxTempPerWindow.print();

        env.execute("Compute min and max temperature");
    }
}