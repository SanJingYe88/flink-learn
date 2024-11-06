package it.com.launcher.state;


import it.com.entity.Event;
import it.com.function.ds.ClickSource;
import it.com.function.state.UserStateTtlFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

/**
 * 测试 State TTL
 */
public class TestStateTTLLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        stream.keyBy(Event::getId)
                .flatMap(new UserStateTtlFlatMapFunction())
                .print();
        env.execute();
    }
}