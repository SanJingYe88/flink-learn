package it.com.launcher.co;

import it.com.entity.Alert;
import it.com.entity.SensorReading;
import it.com.enums.SmokeLevel;
import it.com.function.co.RaiseAlertFlatMap;
import it.com.function.ds.SensorSource;
import it.com.function.ds.SmokeLevelSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 同处理函数
 */
public class TestMultiStreamTransformationsLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 传感器 数据源
        DataStream<SensorReading> sensorReadingDataStream = env.addSource(new SensorSource());
        // 烟雾等级 数据源
        DataStream<SmokeLevel> smokeLevelDataStream = env.addSource(new SmokeLevelSource()).setParallelism(1);

        DataStream<Alert> alerts = sensorReadingDataStream.keyBy(SensorReading::getId)
                .connect(smokeLevelDataStream.broadcast())
                .flatMap(new RaiseAlertFlatMap());

        alerts.print();
        env.execute("Multi-Stream Transformations Example");
    }
}