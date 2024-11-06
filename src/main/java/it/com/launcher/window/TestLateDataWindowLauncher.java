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
public class TestLateDataWindowLauncher {
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
                        Tuple3.of("user_1", 1000L, 4),         // 迟到但在允许范围内
                        Tuple3.of("user_1", 15000L, 5),        // 正常数据
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
         * 数据分析：
         * // 输入数据（按时间戳排序）
         * Tuple3.of("user_1", 1L, 1)      // 窗口[0-5000)的数据
         * Tuple3.of("user_1", 1000L, 4)   // 窗口[0-5000)的数据
         * Tuple3.of("user_1", 2000L, 2)   // 窗口[0-5000)的数据
         * Tuple3.of("user_1", 2000L, 6)   // 窗口[0-5000)的迟到数据
         * Tuple3.of("user_1", 4999L, 3)   // 窗口[0-5000)的数据
         * Tuple3.of("user_1", 15000L, 5)  // 窗口[15000-20000)的数据
         *
         *
         * 处理流程：
         * // 水位线计算
         * 当前水位线 = 最大时间戳 - MAX_OUT_OF_ORDERNESS(2秒)
         *
         * // 窗口处理
         * - 如果 数据时间戳 > 水位线，直接进入对应窗口
         * - 如果 水位线 - ALLOWED_LATENESS < 数据时间戳 < 水位线，更新窗口结果
         * - 如果 数据时间戳 < 水位线 - ALLOWED_LATENESS，进入侧输出流
         *
         *
         * 预期输出：
         * // 主流输出（正常窗口结果）
         * Window Result> Window[0 - 5000] count: 4
         * // 包含时间戳为1L, 1000L, 2000L, 4999L的数据
         *
         * Window Result> Window[15000 - 20000] count: 1
         * // 包含时间戳为15000L的数据
         *
         * // 侧输出流（严重迟到的数据）
         * Late Events> (user_1, 2000, 6)
         * // 因为当水位线推进到15000-2000=13000时，这条数据已经严重迟到
         *
         *
         * 数据处理详解：
         * // 第一个窗口[0-5000)的处理
         * 1L     -> 正常进入窗口
         * 1000L  -> 正常进入窗口
         * 2000L  -> 正常进入窗口
         * 4999L  -> 正常进入窗口
         * 2000L,6 -> 由于严重迟到，进入侧输出流
         *
         * // 第二个窗口[15000-20000)的处理
         * 15000L -> 正常进入窗口
         *
         *
         * 关键时间点：
         * // 水位线变化
         * 初始水位线 = -MAX_OUT_OF_ORDERNESS = -2000
         * 当15000L到达时，水位线提升到 13000
         *
         * // 窗口触发
         * [0-5000)窗口在水位线超过5000时触发
         * [15000-20000)窗口在水位线超过20000时触发
         *
         *
         * 处理机制：
         * // 1. 窗口创建和更新
         * - 数据到达时创建对应的窗口
         * - 在允许延迟时间内可以更新窗口结果
         *
         * // 2. 水位线推进
         * - 随着最大时间戳的数据到达而推进
         * - 决定窗口是否关闭和数据是否迟到
         *
         * // 3. 迟到数据处理
         * - 在允许范围内的迟到数据会更新窗口结果
         * - 超出允许范围的数据会被发送到侧输出流
         *
         *
         *
         *
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