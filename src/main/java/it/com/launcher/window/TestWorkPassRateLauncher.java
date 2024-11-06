package it.com.launcher.window;

import it.com.entity.WorkPassRate;
import it.com.entity.WorkResult;
import it.com.function.window.PassRateAggregateFunction;
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
 * 使用滑动窗口，每10分钟计算一次，窗口大小为24小时
 */
public class TestWorkPassRateLauncher {
    /**
     * 配置常量
     */
    private static final int SOURCE_RATE_PER_SECOND = 10;
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    private static final Duration WINDOW_SIZE = Duration.ofHours(24);
    private static final Duration SLIDE_SIZE = Duration.ofMinutes(10);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置数据源和水印策略
        DataGeneratorSource<WorkResult> source = DataGeneratorSourceUtil.workResultDataGeneratorSource(SOURCE_RATE_PER_SECOND);
        WatermarkStrategy<WorkResult> watermarkStrategy = WatermarkStrategy.<WorkResult>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                .withTimestampAssigner((data, timestamp) -> data.getTs());

        // 处理数据流
        SingleOutputStreamOperator<WorkPassRate> resultStream = env.fromSource(source, watermarkStrategy, "Workflow Result Source")
                .keyBy(WorkResult::getWorkId)
                .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE.toMillis()), Time.milliseconds(SLIDE_SIZE.toMillis())))
                .aggregate(new PassRateAggregateFunction())
                .map(result -> {
                    result.setCalTime(System.currentTimeMillis());
                    result.setPassRate(calculatePassRate(result));
                    return result;
                });

        // 输出结果
        resultStream.print("Workflow Pass Rate");

        // 执行任务
        env.execute("Workflow Pass Rate Calculation");
    }

    /**
     * 计算通过率
     */
    private static double calculatePassRate(WorkPassRate result) {
        if (result.getTotalNum() == 0) {
            return 0.0;
        }
        return (double) result.getPassNum() / result.getTotalNum();
    }
}
