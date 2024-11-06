package it.com.launcher.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 测试：基于窗口的双流 join
 * 将两个数据流在时间窗口内根据key进行关联
 * 本例中关联用户在5秒钟内的点击和浏览事件
 */
public class TestTwoWindowJoin2Launcher {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建点击事件流
        DataStream<Tuple3<String, Long, String>> clickStream = env
                .fromElements(
                        Tuple3.of("user_1", 2000L, "click"),
                        Tuple3.of("user_1", 3000L, "click"),
                        Tuple3.of("user_2", 3000L, "click"),
                        Tuple3.of("user_2", 5000L, "click"),
                        Tuple3.of("user_1", 3000L, "click")
                ).assignTimestampsAndWatermarks(createWatermarkStrategy());

        // 创建浏览事件流
        DataStream<Tuple3<String, Long, String>> browseStream = env
                .fromElements(
                        Tuple3.of("user_1", 2500L, "browse"),
                        Tuple3.of("user_2", 2200L, "browse"),
                        Tuple3.of("user_2", 2500L, "browse"),
                        Tuple3.of("user_2", 6000L, "click"),
                        Tuple3.of("user_2", 10000L, "click"),
                        Tuple3.of("user_2", 11000L, "click"),
                        Tuple3.of("user_2", 12000L, "click"),
                        Tuple3.of("user_2", 120000L, "click"),
                        Tuple3.of("user_1", 3500L, "click")
                ).assignTimestampsAndWatermarks(createWatermarkStrategy());

        // 执行window join
        DataStream<String> joinedStream = clickStream.join(browseStream)
                // clickStream流的key
                .where(click -> click.f0)
                // browseStream流的key
                .equalTo(browse -> browse.f0)
                // 指定WindowAssigner,窗口大小为5秒
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 指定JoinFunction
                .apply(new UserActivityJoinFunction());

        // 打印结果
        joinedStream.print("Joined Stream");

        env.execute("Two Window Join Example");

        /**
         * 数据分布（5秒一个窗口）：
         *
         * // 窗口1 [0-5000)
         * clickStream:
         * - ("user_1", 2000L, "click")
         * - ("user_1", 3000L, "click")  // 重复两次
         * - ("user_2", 3000L, "click")
         *
         * browseStream:
         * - ("user_1", 2500L, "browse")
         * - ("user_1", 3500L, "click")
         * - ("user_2", 2200L, "browse")
         * - ("user_2", 2500L, "browse")
         *
         * // 窗口2 [5000-10000)
         * clickStream:
         * - ("user_2", 5000L, "click")
         *
         * browseStream:
         * - ("user_2", 6000L, "click")
         *
         *
         * 运行结果：
         * Joined Stream> User: user_1, Click Time: 2000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 2000 ms, Browse Time: 3500 ms, Time Diff: 1500 ms
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 3500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 3500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_2, Click Time: 3000 ms, Browse Time: 2200 ms, Time Diff: 800 ms
         * Joined Stream> User: user_2, Click Time: 3000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_2, Click Time: 5000 ms, Browse Time: 6000 ms, Time Diff: 1000 ms
         *
         * 结果分析：
         * // user_1 在窗口1的匹配（2000ms点击事件）
         * Joined Stream> User: user_1, Click Time: 2000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 2000 ms, Browse Time: 3500 ms, Time Diff: 1500 ms
         *
         * // user_1 在窗口1的匹配（第一个3000ms点击事件）
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 3500 ms, Time Diff: 500 ms
         *
         * // user_1 在窗口1的匹配（第二个3000ms点击事件）
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         * Joined Stream> User: user_1, Click Time: 3000 ms, Browse Time: 3500 ms, Time Diff: 500 ms
         *
         * // user_2 在窗口1的匹配
         * Joined Stream> User: user_2, Click Time: 3000 ms, Browse Time: 2200 ms, Time Diff: 800 ms
         * Joined Stream> User: user_2, Click Time: 3000 ms, Browse Time: 2500 ms, Time Diff: 500 ms
         *
         * // user_2 在窗口2的匹配
         * Joined Stream> User: user_2, Click Time: 5000 ms, Browse Time: 6000 ms, Time Diff: 1000 ms
         *
         */
    }

    /**
     * 创建水印策略
     */
    private static WatermarkStrategy<Tuple3<String, Long, String>> createWatermarkStrategy() {
        return WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))  // 允许1秒的数据延迟
                .withTimestampAssigner((event, timestamp) -> event.f1);
    }

    /**
     * 用户活动关联函数
     * 定义如何将两个流中的元素关联在一起
     */
    private static class UserActivityJoinFunction implements JoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String> {

        @Override
        public String join(Tuple3<String, Long, String> click, Tuple3<String, Long, String> browse) {
            return String.format("User: %s, Click Time: %d ms, Browse Time: %d ms, Time Diff: %d ms",
                    click.f0,
                    click.f1,
                    browse.f1,
                    Math.abs(click.f1 - browse.f1));
        }
    }
}
