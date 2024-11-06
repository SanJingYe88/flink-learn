package it.com.function.window;

import it.com.entity.MinMaxTemp;
import it.com.entity.SensorReading;
import it.com.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.DoubleSummaryStatistics;
import java.util.stream.StreamSupport;

/**
 * 温度最大最小值处理函数
 * 用于计算每个传感器在窗口期内的最高温度和最低温度
 */
public class HighAndLowTempProcessFunction extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {

    /**
     * 处理窗口数据
     *
     * @param key      传感器ID
     * @param context  窗口上下文，包含窗口信息
     * @param elements 窗口内的所有传感器读数
     * @param out      结果收集器
     */
    @Override
    public void process(String key, Context context, Iterable<SensorReading> elements, Collector<MinMaxTemp> out) {

        // 1. 获取窗口信息
        TimeWindow window = context.window();
        System.out.println("Window Start: " + DateUtil.formatHHmmss(window.getStart()));
        System.out.println("Window End: " + DateUtil.formatHHmmss(window.getEnd()));
        System.out.println("Window MaxTimestamp: " + DateUtil.formatHHmmss(window.maxTimestamp()));

        // 2. 获取当前水印
        long watermark = context.currentWatermark();
        System.out.println("Current Watermark: " + DateUtil.formatHHmmss(watermark));

        // 3. 获取当前处理时间
        long processingTime = context.currentProcessingTime();
        System.out.println("Processing Time: " + DateUtil.formatHHmmss(processingTime));

        // 4. 使用状态
        // 获取全局状态
        context.globalState().getMapState(null);
        // 获取窗口状态
        context.windowState().getMapState(null);

        // 5. 访问侧输出
        context.output(null, "Side output data");

        // 6. 获取窗口大小
        long windowSize = window.getEnd() - window.getStart();
        System.out.println("Window Size: " + windowSize + "ms");

        // 7. 判断窗口是否包含某个时间戳
        long someTimestamp = System.currentTimeMillis();
        boolean containsTimestamp = window.getStart() <= someTimestamp && someTimestamp < window.getEnd();
        System.out.println("Window contains current timestamp: " + containsTimestamp);

        // 处理窗口数据，计算最大最小值
        DoubleSummaryStatistics stats = StreamSupport.stream(elements.spliterator(), false)
                .mapToDouble(SensorReading::getTemperature).summaryStatistics();

        // 输出结果
        out.collect(new MinMaxTemp(key, stats.getMin(), stats.getMax(), window.getEnd()));
    }
}