package it.com.launcher.window;

import it.com.entity.SensorReading;
import it.com.function.ds.SensorSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 测试 计算每个传感器15s窗口中的温度最小值。 滚动处理时间+ReduceFunction
 */
public class TestTumblingProcessingTimeWindowsReduceFunctionLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据
        DataStreamSource<SensorReading> dataStream = env.addSource(new SensorSource());

        WindowedStream<SensorReading, String, TimeWindow> windowedStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));

        SingleOutputStreamOperator<SensorReading> singleOutputStreamOperator = windowedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                if (value1.getTemperature() < value2.getTemperature()) {
                    return value1;
                } else {
                    return value2;
                }
            }
        });

        dataStream.print("生产出元素：");
        singleOutputStreamOperator.print("15s最低温度：");

        env.execute();
    }
}
