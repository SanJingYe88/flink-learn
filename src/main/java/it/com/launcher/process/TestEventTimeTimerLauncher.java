package it.com.launcher.process;

import it.com.entity.Event;
import it.com.function.ds.CustomSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 测试 基于 KeyedProcessFunction 的定时处理，事件时间
 */
public class TestEventTimeTimerLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));
        // 基于 KeyedStream 定义事件时间定时器
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                        out.collect(" 数据到达，水位线为： " + ctx.timerService().currentWatermark());
                        // 根据事件时间，注册一个 10 秒后的定时器
                        long eventTime = value.getTs();
                        ctx.timerService().registerEventTimeTimer(eventTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("------定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                })
                .print();
        env.execute();
    }
}