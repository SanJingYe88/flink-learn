package it.com.launcher;

import it.com.entity.User;
import it.com.function.ds.UserDataGeneratorSource;
import it.com.function.ds.UserDelayDataGeneratorSource;
import it.com.watermark.PunctuatedWatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 使用 自定义周期性 Watermark 生成器
 */
public class TestPunctuatedWatermarkStrategyLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //  设置周期性生成水位线的时间间隔(默认为200毫秒),这里设置为1s
        env.getConfig().setAutoWatermarkInterval(1000L);
        // 自定义的 DataGeneratorSource，需要实现 SourceFunction 接口
        UserDelayDataGeneratorSource userDelayDataGeneratorSource = new UserDelayDataGeneratorSource(10);
        DataStreamSource<User> userDataStreamSource = env.addSource(userDelayDataGeneratorSource);
        // 设置水位线规则,自定义周期性生成
        SingleOutputStreamOperator<User> userSingleOutputStreamOperator = userDataStreamSource.assignTimestampsAndWatermarks(new PunctuatedWatermarkStrategy());
        userSingleOutputStreamOperator.print();
        env.execute();
    }
}
