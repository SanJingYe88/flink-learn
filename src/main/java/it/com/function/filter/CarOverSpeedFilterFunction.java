package it.com.function.filter;

import it.com.entity.CarOverSpeedLimitInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;

@Slf4j
public class CarOverSpeedFilterFunction extends RichFilterFunction<CarOverSpeedLimitInfo> {

    @Override
    public boolean filter(CarOverSpeedLimitInfo carOverSpeedLimitInfo) {
        if (carOverSpeedLimitInfo == null) {
            return false;
        }
        return carOverSpeedLimitInfo.getCurrentSpeed() > carOverSpeedLimitInfo.getLimitSpeed() * 1.2;
    }
}
