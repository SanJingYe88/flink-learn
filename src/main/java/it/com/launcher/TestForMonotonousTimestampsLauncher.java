package it.com.launcher;

import it.com.entity.User;
import it.com.function.ds.UserDataGeneratorSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 内置有序水位线生成器 forMonotonousTimestamps
 */
public class TestForMonotonousTimestampsLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每秒生成一次水位线
        env.getConfig().setAutoWatermarkInterval(1000L);
        // 自定义的 DataGeneratorSource，需要实现 SourceFunction 接口
        UserDataGeneratorSource userDataGeneratorSource = new UserDataGeneratorSource(5);
        DataStreamSource<User> userDataStreamSource = env.addSource(userDataGeneratorSource);

        // 创建内置有序水位线生成器
        WatermarkStrategy<User> userWatermarkStrategy = WatermarkStrategy.<User>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<User>() {
            @Override
            public long extractTimestamp(User user, long l) {
                return user.getTs();
            }
        });

        // 使用 lambda 写法
        userWatermarkStrategy = WatermarkStrategy.<User>forMonotonousTimestamps().withTimestampAssigner((user, l) -> user.getTs());

        SingleOutputStreamOperator<User> userSingleOutputStreamOperator = userDataStreamSource.assignTimestampsAndWatermarks(userWatermarkStrategy);
        userSingleOutputStreamOperator.print();
        env.execute();

    }
}
