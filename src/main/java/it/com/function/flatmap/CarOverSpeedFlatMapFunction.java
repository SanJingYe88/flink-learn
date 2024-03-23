package it.com.function.flatmap;

import it.com.entity.CarOverSpeedLimitInfo;
import it.com.util.JsonUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class CarOverSpeedFlatMapFunction extends RichFlatMapFunction<CarOverSpeedLimitInfo, String> {
    @Override
    public void flatMap(CarOverSpeedLimitInfo carOverSpeedLimitInfo, Collector<String> collector) throws Exception {
        if (carOverSpeedLimitInfo == null) {
            return;
        }
        collector.collect(JsonUtils.toJson(carOverSpeedLimitInfo));
    }
}
