package it.com.launcher.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 处理迟到数据的示例
 * 使用侧输出流收集迟到的数据
 */
public class TestLateDataWindow2Launcher {
    /**
     * 窗口大小为5秒
     */
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(5);

    /**
     * 允许5秒的迟到
     */
    private static final Duration ALLOWED_LATENESS = Duration.ofSeconds(5);

    /**
     * 水位线延迟2秒
     */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(2);

    /**
     * 侧输出流标签，用于收集迟到的数据
     */
    private static final OutputTag<Tuple3<String, Long, Integer>> LATE_DATA_TAG = new OutputTag<Tuple3<String, Long, Integer>>("late-data") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建数据流
        DataStream<Tuple3<String, Long, Integer>> stream = env
                .fromElements(
                        Tuple3.of("user_1", 1L, 1),            // 正常数据
                        Tuple3.of("user_1", 2000L, 2),         // 正常数据
                        Tuple3.of("user_1", 4999L, 3),         // 正常数据
                        Tuple3.of("user_1", 5001L, 3),
                        Tuple3.of("user_1", 6001L, 3),
                        Tuple3.of("user_1", 7001L, 3),
                        Tuple3.of("user_1", 5100L, 3),
                        Tuple3.of("user_1", 5010L, 3),
                        Tuple3.of("user_1", 7001L, 3),
                        Tuple3.of("user_1", 6100L, 3),
                        Tuple3.of("user_1", 8001L, 3),
                        Tuple3.of("user_1", 11001L, 3),
                        Tuple3.of("user_1", 10000L, 4),
                        Tuple3.of("user_1", 9000L, 4),         // 迟到但在允许范围内
                        Tuple3.of("user_1", 15000L, 5),        // 正常数据
                        Tuple3.of("user_1", 13000L, 5),
                        Tuple3.of("user_1", 2000L, 6)          // 严重迟到的数据
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS) // 水位线延迟2秒
                                .withTimestampAssigner((event, timestamp) -> event.f1)
                );

        // 处理数据流
        SingleOutputStreamOperator<String> result = stream
                .keyBy(data -> data.f0)
                // 滚动事件时间，窗口大小为5s
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE.getSeconds())))
                // 允许数据迟到, 允许5秒的迟到
                .allowedLateness(Time.seconds(ALLOWED_LATENESS.getSeconds()))
                // 将迟到的数据发送到侧输出流
                .sideOutputLateData(LATE_DATA_TAG)
                .process(new LateDataProcessFunction());

        // 打印主流结果
        result.print("Window Result");

        // 打印迟到的数据
        result.getSideOutput(LATE_DATA_TAG).print("Late Events");

        env.execute("Late Data Handling Example");


        /**
         * 每条数据到来时的状态：
         * 初始数据进入：
         * // 1. Tuple3.of("user_1", 1L, 1)
         * 当前最大时间戳: 1L
         * 数据分配: 进入窗口[0-5000)
         * 当前水位线: -1999ms (1L - 2000ms)
         * 窗口状态: [0-5000) 窗口活跃，包含1条数据
         *
         * // 2. Tuple3.of("user_1", 2000L, 2)
         * 当前最大时间戳: 2000L
         * 数据分配: 进入窗口[0-5000)
         * 当前水位线: 0ms (2000L - 2000ms)
         * 窗口状态: [0-5000) 窗口活跃，包含2条数据
         *
         * // 3. Tuple3.of("user_1", 4999L, 3)
         * 当前最大时间戳: 4999L
         * 数据分配: 进入窗口[0-5000)
         * 当前水位线: 2999ms (4999L - 2000ms)
         * 窗口状态: [0-5000) 窗口活跃，包含3条数据
         *
         * 后续数据处理：
         * // 4. Tuple3.of("user_1", 5001L, 3)
         * 当前最大时间戳: 5001L
         * 数据分配: 进入窗口[5000-10000)
         * 当前水位线: 3001ms (5001L - 2000ms)
         * 窗口状态: [0-5000) 窗口活跃，包含3条数据
         *          [5000-10000) 窗口活跃，包含1条数据
         *
         * // 5. Tuple3.of("user_1", 6001L, 3)
         * 当前最大时间戳: 6001L
         * 数据分配: 进入窗口[5000-10000)
         * 当前水位线: 4001ms (6001L - 2000ms)
         * 窗口状态: [0-5000) 窗口活跃，包含3条数据
         *          [5000-10000) 窗口活跃，包含2条数据
         *
         * // 6. Tuple3.of("user_1", 7001L, 3)
         * 当前最大时间戳: 7001L
         * 数据分配: 进入窗口[5000-10000)
         * 当前水位线: 5001ms (7001L - 2000ms)
         * 窗口状态: [0-5000) 窗口第一次触发，输出count:3（因为水位线超过5000）
         *          [5000-10000) 窗口活跃，包含3条数据
         *
         *
         * 更多数据进入：
         * // 7. Tuple3.of("user_1", 5100L, 3)
         * 当前最大时间戳: 7001L（不变）
         * 数据分配: 进入窗口[5000-10000)
         * 当前水位线: 5001ms（不变）
         * 窗口状态: [0-5000) 窗口可更新（在允许延迟期内）
         *          [5000-10000) 窗口活跃，包含4条数据
         *
         * // 8. Tuple3.of("user_1", 5010L, 3)
         * 数据分配: 进入窗口[5000-10000)
         * 当前最大时间戳: 7001L（不变）
         * 当前水位线: 5001ms（不变）
         * 窗口状态: [0-5000) 窗口可更新（在允许延迟期内）
         *          [5000-10000) 窗口活跃，包含5条数据
         *
         * 后续窗口触发：
         * // 当数据时间戳达到12000L时
         * 当前最大时间戳: 12000L
         * 当前水位线: 10000ms (12000L - 2000ms)
         * 窗口状态: [0-5000) 窗口关闭（超过允许延迟时间）
         *          [5000-10000) 窗口第一次触发，输出count
         *          [10000-15000) 窗口活跃
         */
    }


    private static class LateDataProcessFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<String> out) throws Exception {
            long count = 0;
            for (Tuple3<String, Long, Integer> element : elements) {
                count++;
            }
            TimeWindow window = context.window();
            out.collect(String.format("Window[%d - %d] count: %d", window.getStart(), window.getEnd(), count));
        }
    }
} 