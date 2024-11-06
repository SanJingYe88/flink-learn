package it.com.launcher.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 测试：基于间隔的双流 join
 * <p>
 * 用于查找在指定时间区间内发生的相关事件
 * 本例中查找用户点击事件前10分钟内的浏览记录
 */
public class TestIntervalJoinLauncher {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建点击事件流
        KeyedStream<Tuple3<String, Long, String>, String> clickStream = env.fromElements(Tuple3.of("user_1", 10 * 60 * 1000L, "click")) // 10分钟时的点击事件
                .assignTimestampsAndWatermarks(createWatermarkStrategy())
                .keyBy(r -> r.f0);

        // 创建浏览事件流
        KeyedStream<Tuple3<String, Long, String>, String> browseStream = env
                .fromElements(
                        Tuple3.of("user_1", 5 * 60 * 1000L, "browse"),  // 5分钟时的浏览事件
                        Tuple3.of("user_1", 6 * 60 * 1000L, "browse")   // 6分钟时的浏览事件
                )
                .assignTimestampsAndWatermarks(createWatermarkStrategy())
                .keyBy(r -> r.f0);

        // 执行 join
        clickStream.intervalJoin(browseStream)
                // 向前查找10分钟，到当前时间
                .between(Time.minutes(-10), Time.minutes(0))
                .process(new UserActivityJoinFunction())
                .print();

        /**
         * 数据分析：
         * // 点击事件
         * clickStream:
         * - ("user_1", 600000L, "click")     // 10分钟时的点击事件
         *
         * // 浏览事件
         * browseStream:
         * - ("user_1", 300000L, "browse")    // 5分钟时的浏览事件
         * - ("user_1", 360000L, "browse")    // 6分钟时的浏览事件
         *
         * // 时间区间设置
         * .between(Time.minutes(-10), Time.minutes(0))
         * // 对于10分钟的点击事件，会查找 [0分钟, 10分钟] 范围内的浏览事件
         *
         *
         * 运行结果：
         * User: user_1, Click at: 600000 ms, Browse at: 300000 ms
         * User: user_1, Click at: 600000 ms, Browse at: 360000 ms
         *
         *
         * 结果分析：
         * // 第一条匹配
         * Click at: 600000 ms (10分钟)
         * Browse at: 300000 ms (5分钟)
         * 原因：5分钟的浏览事件在10分钟点击事件的前10分钟范围内
         *
         * // 第二条匹配
         * Click at: 600000 ms (10分钟)
         * Browse at: 360000 ms (6分钟)
         * 原因：6分钟的浏览事件也在10分钟点击事件的前10分钟范围内
         */
        env.execute("Interval Join Example");
    }

    /**
     * 创建水印策略
     */
    private static WatermarkStrategy<Tuple3<String, Long, String>> createWatermarkStrategy() {
        return WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.f1);
    }

    /**
     * 用户活动关联处理函数
     */
    private static class UserActivityJoinFunction extends ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String> {

        @Override
        public void processElement(Tuple3<String, Long, String> click, Tuple3<String, Long, String> browse, Context context, Collector<String> collector) {
            String result = String.format("User: %s, Click at: %d ms, Browse at: %d ms", click.f0, click.f1, browse.f1);
            collector.collect(result);
        }
    }
}