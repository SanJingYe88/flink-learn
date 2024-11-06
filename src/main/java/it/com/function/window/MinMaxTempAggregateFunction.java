package it.com.function.window;

import it.com.entity.MinMaxTempAcc;
import it.com.entity.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 温度最大最小值聚合函数
 * 增量计算每个窗口的最高温度和最低温度
 */
public class MinMaxTempAggregateFunction implements AggregateFunction<SensorReading, MinMaxTempAcc, MinMaxTempAcc> {

    @Override
    public MinMaxTempAcc createAccumulator() {
        return new MinMaxTempAcc(Double.MAX_VALUE, Double.MIN_VALUE);
    }

    @Override
    public MinMaxTempAcc add(SensorReading value, MinMaxTempAcc acc) {
        return new MinMaxTempAcc(
                Math.min(acc.getMin(), value.getTemperature()),
                Math.max(acc.getMax(), value.getTemperature())
        );
    }

    @Override
    public MinMaxTempAcc getResult(MinMaxTempAcc acc) {
        return acc;
    }

    @Override
    public MinMaxTempAcc merge(MinMaxTempAcc acc1, MinMaxTempAcc acc2) {
        return new MinMaxTempAcc(
                Math.min(acc1.getMin(), acc2.getMin()),
                Math.max(acc1.getMax(), acc2.getMax())
        );
    }
}