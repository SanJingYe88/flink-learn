package it.com.launcher.window;

import it.com.entity.Event;
import it.com.function.ds.ClickSource;
import it.com.util.DateUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.Types;

import java.time.Duration;

/**
 * 测试 统计5s内的点击次数。 滚动事件时间+ReduceFunction
 */
public class TestTumblingEventTimeWindowsReduceFunctionLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(TestTumblingEventTimeWindowsReduceFunctionLauncher.class);
    /**
     * 窗口大小
     */
    private static final int WINDOW_SIZE_SECONDS = 5;
    /**
     * 允许3秒的数据延迟
     */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(3);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 在执行之前打印启动信息
        // Flink 程序的执行是懒加载的，在调用 execute() 之前，所有的转换操作都只是在构建执行计划
        // env.execute() 才是真正开始执行 Flink 作业的调用
        System.out.println("启动程序："+ DateUtil.formatHHmmss(System.currentTimeMillis()));

        // 配置水印策略
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, timestamp) -> event.getTs());

        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        stream
                .map(event -> Tuple2.of(event.getUrl(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
                .reduce(
                        // 定义累加规则
                        (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1),
                        // 窗口处理逻辑
                        new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
                            // 使用ProcessWindowFunction与ReduceFunction结合，这样我们可以在窗口触发时获取当前系统时间
                            // 在process方法中，我们可以访问窗口的上下文信息，并在实际输出结果时获取当前系统时间
                            // 移除了原来的print语句，改为在ProcessWindowFunction中输出结果
                            // 这样修改后，每次窗口触发计算时都会显示当前的实时时间，而不是程序启动时的固定时间。
                            @Override
                            public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements,
                                                Collector<Tuple2<String, Long>> out) {
                                Tuple2<String, Long> result = elements.iterator().next();
                                TimeWindow window = context.window();
                                System.out.println(String.format("Window[%s-%s] URL: %s, Count: %d", DateUtil.formatHHmmss(window.getStart()), DateUtil.formatHHmmss(window.getEnd()), result.f0, result.f1));
                                out.collect(result);
                            }
                        });

        stream.print("生产出元素：");
        // 在 Flink 程序中，env.execute() 是一个阻塞调用，它会一直运行直到作业被手动停止或者发生异常, 任何放在 execute() 之后的代码都不会被执行到
        env.execute();
    }
}
