package it.com.function.window;

import it.com.entity.WorkPassRate;
import it.com.entity.WorkResult;
import it.com.util.DateUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 工作通过率窗口函数
 */
public class WorkPassRateWindowFunction implements WindowFunction<WorkResult, WorkPassRate, String, TimeWindow> {

    @Override
    public void apply(String workId,
                      TimeWindow window,
                      Iterable<WorkResult> input,
                      Collector<WorkPassRate> out) {

        // 获取窗口信息
        long windowStart = window.getStart();
        long windowEnd = window.getEnd();
        long windowMaxTimestamp = window.maxTimestamp();

        System.out.println(String.format("Window[%s-%s] processing data for workId: %s", DateUtil.formatHHmmss(windowStart), DateUtil.formatHHmmss(windowEnd), workId));

        int totalNum = 0;
        int passNum = 0;

        // 遍历窗口中的所有数据
        for (WorkResult workResult : input) {
            totalNum++;
            if (workResult.getPassFlag() == 1) {
                passNum++;
            }
            // 可以获取每条数据的时间戳
            System.out.println("Processing record with timestamp: " + DateUtil.formatHHmmss(workResult.getTs()));
        }

        // 创建结果对象
        WorkPassRate workPassRate = new WorkPassRate();
        workPassRate.setWorkId(workId);
        workPassRate.setTotalNum(totalNum);
        workPassRate.setPassNum(passNum);
        workPassRate.setPassRate(totalNum == 0 ? 0.0 : (double) passNum / totalNum);
        workPassRate.setCalTime(System.currentTimeMillis());

        // 可以将窗口信息添加到结果中
        workPassRate.setWindowStart(windowStart);
        workPassRate.setWindowEnd(windowEnd);

        out.collect(workPassRate);
    }
}