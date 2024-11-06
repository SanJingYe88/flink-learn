package it.com.launcher.state;

import it.com.entity.Event;
import it.com.function.ds.ClickSource;
import it.com.function.state.PeriodicPvFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class TestPeriodicPvLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                );
        //原始数据
        stream.print("input");

        //统计每个用户的PV，统计当前用户的访问次数，做一个累计一个聚合，输出到定时器
        stream.keyBy(Event::getName)
                .process(new PeriodicPvFunction())
                .print();

        env.execute();
    }
}
