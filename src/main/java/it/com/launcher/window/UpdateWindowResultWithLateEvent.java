package it.com.launcher.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 演示窗口更新和延迟数据处理
 * 使用窗口状态来跟踪窗口的更新次数
 */
public class UpdateWindowResultWithLateEvent {
    // 配置常量
    private static final Duration ALLOWED_LATENESS = Duration.ofSeconds(5);
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        // 环境设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建数据流并处理
        SingleOutputStreamOperator<String> result = env
                .socketTextStream("localhost", 9999)
                .process(new DataParser())  // 解析输入数据
                .assignTimestampsAndWatermarks(createWatermarkStrategy())
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE.toMillis())))
                .allowedLateness(Time.milliseconds(ALLOWED_LATENESS.toMillis()))
                .process(new WindowUpdateTracker());

        // 打印结果
        result.print();

        env.execute("Window Update Demo");
    }

    /**
     * 创建水印策略
     */
    private static WatermarkStrategy<Tuple2<String, Long>> createWatermarkStrategy() {
        return WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, timestamp) -> event.f1);
    }

    /**
     * 数据解析函数
     */
    private static class DataParser extends ProcessFunction<String, Tuple2<String, Long>> {
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            try {
                String[] arr = value.split(" ");
                if (arr.length == 2) {
                    String key = arr[0];
                    long timestamp = Long.parseLong(arr[1]) * 1000L;
                    out.collect(Tuple2.of(key, timestamp));

                    // 打印输入数据信息
                    System.out.println(String.format("收到数据 => Key: %s, Timestamp: %d", key, timestamp));
                }
            } catch (Exception e) {
                System.err.println("数据解析错误: " + value);
            }
        }
    }

    /**
     * 窗口处理函数
     * 跟踪窗口更新次数和处理延迟数据
     */
    private static class WindowUpdateTracker extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {
        private ValueState<Integer> updateCount;

        @Override
        public void open(Configuration parameters) {
            // 初始化状态
            updateCount = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("updateCount", Types.INT)
            );
        }

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out)
                throws Exception {
            // 获取窗口信息
            TimeWindow window = context.window();
            long watermark = context.currentWatermark();

            // 统计数据
            long count = 0L;
            for (Tuple2<String, Long> element : elements) {
                count += 1;
            }

            // 获取更新次数
            int currentCount = updateCount.value() == null ? 0 : updateCount.value();
            updateCount.update(currentCount + 1);

            // 生成输出信息
            String result = String.format("窗口[%d - %d] 第%d次触发计算! " +
                            "Key: %s, 数据量: %d, 当前水位线: %d",
                    window.getStart(), window.getEnd(),
                    currentCount + 1,
                    key, count,
                    watermark);

            out.collect(result);
        }
    }
}

