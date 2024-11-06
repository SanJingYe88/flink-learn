package it.com.function.window;

import it.com.entity.MinMaxTemp;
import it.com.entity.MinMaxTempAcc;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 温度最大最小值处理窗口函数
 * 将聚合结果转换为最终输出格式，并添加窗口信息
 */
public class MinMaxTempProcessWindowFunction extends ProcessWindowFunction<MinMaxTempAcc, MinMaxTemp, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<MinMaxTempAcc> elements, Collector<MinMaxTemp> out) {
        // 由于使用了AggregateFunction，这里的elements只会有一个元素
        MinMaxTempAcc acc = elements.iterator().next();

        out.collect(new MinMaxTemp(key, acc.getMin(), acc.getMax(), context.window().getEnd()));
    }
}