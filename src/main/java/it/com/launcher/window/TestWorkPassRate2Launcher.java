package it.com.launcher.window;

import it.com.entity.WorkPassRate;
import it.com.entity.WorkResult;
import it.com.function.window.WorkPassRateWindowFunction;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 工作通过率计算
 */
public class TestWorkPassRate2Launcher {
    /**
     * 配置常量
     */
    private static final int SOURCE_RATE_PER_SECOND = 10;
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(1);
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);
    private static final Duration SLIDE_SIZE = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置数据源和水印策略
        DataGeneratorSource<WorkResult> source = DataGeneratorSourceUtil.workResultDataGeneratorSource(SOURCE_RATE_PER_SECOND);
        WatermarkStrategy<WorkResult> watermarkStrategy = WatermarkStrategy.<WorkResult>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((data, timestamp) -> data.getTs());

        // 处理数据流
        SingleOutputStreamOperator<WorkPassRate> resultStream = env.fromSource(source, watermarkStrategy, "Workflow Result Source")
                .keyBy(WorkResult::getWorkId)
                .window(SlidingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE.toMillis()), Time.milliseconds(SLIDE_SIZE.toMillis())))
                .apply(new WorkPassRateWindowFunction());

        // 输出结果
        resultStream.print("Workflow Pass Rate");

        // 执行任务
        env.execute("Workflow Pass Rate Calculation");
    }
}
