package it.com.launcher.state;


import it.com.entity.ViewTimeLenEvent;
import it.com.function.ds.ViewTimeLenEventSource;
import it.com.function.state.UserViewAvgTimeLenFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 测试 聚合状态
 */
public class TestAverageTimestampLauncher {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 数据
        DataStreamSource<ViewTimeLenEvent> viewTimeLenEventDataStreamSource = env.addSource(new ViewTimeLenEventSource());

        SingleOutputStreamOperator<ViewTimeLenEvent> stream = viewTimeLenEventDataStreamSource
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ViewTimeLenEvent>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<ViewTimeLenEvent>() {
                                    @Override
                                    public long extractTimestamp(ViewTimeLenEvent element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );
        stream.print("input");

        // 自定义实现页面平均浏览时间的统计
        stream.keyBy(ViewTimeLenEvent::getId)
                .flatMap(new UserViewAvgTimeLenFunction(5L))
                .print();

        env.execute();
    }
}

