package it.com.launcher.co;
import it.com.entity.SensorReading;
import it.com.function.co.SwitchCoProcessFunction;
import it.com.function.ds.SensorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 CoProcessFunction
 */
public class TestSensorSwitchLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<SensorReading, String> stream = env.addSource(new SensorSource()).keyBy(SensorReading::getId);

        KeyedStream<Tuple2<String, Long>, String> switches = env.fromElements(Tuple2.of("sensor_2", 10 * 1000L)).keyBy(r -> r.f0);

        stream.connect(switches).process(new SwitchCoProcessFunction()).print("result");

        env.execute();
    }
}

