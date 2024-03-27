package it.com.launcher;

import it.com.entity.User;
import it.com.function.ds.UserDataGeneratorSource;
import it.com.watermark.PeriodWatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试自定义，标记 Watermark 生成器。 每条数据到来后，都会为其生成一条 Watermark。
 */
public class TestPeriodWatermarkStrategyLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 自定义的 DataGeneratorSource，需要实现 SourceFunction 接口
        UserDataGeneratorSource userDataGeneratorSource = new UserDataGeneratorSource(3);
        DataStreamSource<User> userDataStreamSource = env.addSource(userDataGeneratorSource);
        // 设置水位线规则
        SingleOutputStreamOperator<User> userSingleOutputStreamOperator = userDataStreamSource.assignTimestampsAndWatermarks(new PeriodWatermarkStrategy());
        userSingleOutputStreamOperator.print();
        env.execute();

        /**
         * 2024-03-27 22:56:05,183:INFO [] akka.event.slf4j.Slf4jLogger$$anonfun$receive$1 flink-akka.actor.default-dispatcher-4 (Slf4jLogger.scala:107) - Slf4jLogger started
         * 2024-03-27 22:56:05,329:INFO [] akka.event.slf4j.Slf4jLogger$$anonfun$receive$1 flink-metrics-7 (Slf4jLogger.scala:107) - Slf4jLogger started
         * 2024-03-27 22:56:08,211:INFO [] it.com.function.ds.UserDataGeneratorSource Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (UserDataGeneratorSource.java:38) - 生成元素：{"id":61,"name":"bfa9","country":null,"ts":1711551367958}
         * 2024-03-27 22:56:08,211:INFO [] it.com.watermark.PeriodWatermarkStrategy$1 Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (PeriodWatermarkStrategy.java:32) - 当前元素，提取事件时间:1711551367958, recordTimestamp:-9223372036854775808
         * 2024-03-27 22:56:08,215:INFO [] it.com.watermark.PeriodWatermarkGenerator Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (PeriodWatermarkGenerator.java:23) - 发射水位线：User(id=61, name=bfa9, country=null, ts=1711551367958),eventTimestamp:1711551367958
         * 1> User(id=61, name=bfa9, country=null, ts=1711551367958)
         * 2024-03-27 22:56:09,065:INFO [] it.com.function.ds.UserDataGeneratorSource Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (UserDataGeneratorSource.java:38) - 生成元素：{"id":7,"name":"d882","country":null,"ts":1711551369065}
         * 2024-03-27 22:56:09,065:INFO [] it.com.watermark.PeriodWatermarkStrategy$1 Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (PeriodWatermarkStrategy.java:32) - 当前元素，提取事件时间:1711551369065, recordTimestamp:-9223372036854775808
         * 2024-03-27 22:56:09,065:INFO [] it.com.watermark.PeriodWatermarkGenerator Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (PeriodWatermarkGenerator.java:23) - 发射水位线：User(id=7, name=d882, country=null, ts=1711551369065),eventTimestamp:1711551369065
         * 2> User(id=7, name=d882, country=null, ts=1711551369065)
         * 2024-03-27 22:56:09,741:INFO [] it.com.function.ds.UserDataGeneratorSource Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (UserDataGeneratorSource.java:38) - 生成元素：{"id":71,"name":"db3f","country":null,"ts":1711551369741}
         * 2024-03-27 22:56:09,741:INFO [] it.com.watermark.PeriodWatermarkStrategy$1 Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (PeriodWatermarkStrategy.java:32) - 当前元素，提取事件时间:1711551369741, recordTimestamp:-9223372036854775808
         * 2024-03-27 22:56:09,741:INFO [] it.com.watermark.PeriodWatermarkGenerator Legacy Source Thread - Source: Custom Source -> Timestamps/Watermarks (1/1)#0 (PeriodWatermarkGenerator.java:23) - 发射水位线：User(id=71, name=db3f, country=null, ts=1711551369741),eventTimestamp:1711551369741
         * 3> User(id=71, name=db3f, country=null, ts=1711551369741)
         *
         * Process finished with exit code 0
         */
    }
}
