package it.com.launcher;

import it.com.entity.User;
import it.com.function.ds.UserDelayDataGeneratorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 测试：内置乱序流水位线生成器 forBoundedOutOfOrderness
 */
public class TestForBoundedOutOfOrdernessLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  设置周期性生成水位线的时间间隔(默认为200毫秒)
        env.getConfig().setAutoWatermarkInterval(3 * 1000L);
        UserDelayDataGeneratorSource userDelayDataGeneratorSource = new UserDelayDataGeneratorSource(10);
        DataStreamSource<User> userDataStreamSource = env.addSource(userDelayDataGeneratorSource);

        // 创建内置无序水位线生成器
        SingleOutputStreamOperator<User> userSingleOutputStreamOperator = userDataStreamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(1)) // 设置最大乱序时间为1s
                        .withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                            @Override
                            public long extractTimestamp(User user, long l) {
                                return user.getTs();
                            }
                        })
        );

        // 使用 lambda 写法
        SingleOutputStreamOperator<User> userSingleOutputStreamOperator1 = userDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner((user, l) -> user.getTs()));

        userSingleOutputStreamOperator.print();

        // 3.触发程序执行
        env.execute();
    }
}
