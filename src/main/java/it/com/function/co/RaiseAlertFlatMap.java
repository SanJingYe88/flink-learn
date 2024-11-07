package it.com.function.co;

import it.com.entity.Alert;
import it.com.entity.SensorReading;
import it.com.enums.SmokeLevel;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 同处理函数
 */
public class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

    private SmokeLevel smokeLevel = SmokeLevel.LOW;

    @Override
    public void flatMap1(SensorReading tempReading, Collector<Alert> out) throws Exception {
        // high chance of fire => true
        if (this.smokeLevel == SmokeLevel.HIGH && tempReading.getTemperature() > 100) {
            out.collect(new Alert("Risk of fire! " + tempReading, tempReading.getTimestamp()));
        }
    }

    @Override
    public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> out) {
        // update smoke level
        this.smokeLevel = smokeLevel;
    }
}
