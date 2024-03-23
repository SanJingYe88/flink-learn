package it.com.function;

import it.com.entity.CarOverSpeedLimitInfo;
import it.com.entity.CarSpeedInfo;
import it.com.entity.StreetSpeedLimitRule;
import it.com.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class StreetSpeedLimitRuleBroadcastProcessFunction extends BroadcastProcessFunction<CarSpeedInfo, StreetSpeedLimitRule, CarOverSpeedLimitInfo> {

    private final MapStateDescriptor<String, StreetSpeedLimitRule> mapStateDescriptor;

    public StreetSpeedLimitRuleBroadcastProcessFunction(MapStateDescriptor<String, StreetSpeedLimitRule> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    public static String buildMapStateKey(int cityId, int streetId, int crossroadId) {
        return cityId + "_" + streetId + "_" + crossroadId;
    }

    // 对汽车速度信息处理
    @Override
    public void processElement(CarSpeedInfo carSpeedInfo, BroadcastProcessFunction<CarSpeedInfo, StreetSpeedLimitRule, CarOverSpeedLimitInfo>.ReadOnlyContext readOnlyContext, Collector<CarOverSpeedLimitInfo> collector) throws Exception {
        log.info("carSpeedInfo:{}", carSpeedInfo);
        // 获取 mapStateDescriptor
        ReadOnlyBroadcastState<String, StreetSpeedLimitRule> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = buildMapStateKey(carSpeedInfo.getCityId(), carSpeedInfo.getStreetId(), carSpeedInfo.getCrossroadId());
        // 从 ctx 获取 城市_街道_路口 的限速规则
        StreetSpeedLimitRule streetSpeedLimitRule = broadcastState.get(key);
        // 存在限速规则
        if (streetSpeedLimitRule != null) {
            double speedLimit = streetSpeedLimitRule.getLimitSpeed();
            double currentSpeed = carSpeedInfo.getCurrentSpeed();
            if (currentSpeed > speedLimit * 1.2) {
                log.info("地区：{}，当前限速：{}，当前汽车：{}，车速：{}，超过限速，时间:{}", key, speedLimit, carSpeedInfo.getCarNo(), carSpeedInfo.getCurrentSpeed(), System.currentTimeMillis());
                CarOverSpeedLimitInfo carOverSpeedLimitInfo = new CarOverSpeedLimitInfo();
                carOverSpeedLimitInfo.setId(String.valueOf(RandomUtil.generateRandomId()));
                carOverSpeedLimitInfo.setCurrentSpeed(carSpeedInfo.getCurrentSpeed());
                carOverSpeedLimitInfo.setLimitSpeed(streetSpeedLimitRule.getLimitSpeed());
                carOverSpeedLimitInfo.setCarNo(carSpeedInfo.getCarNo());
                carOverSpeedLimitInfo.setStreetId(carSpeedInfo.getStreetId());
                carOverSpeedLimitInfo.setCrossroadId(carSpeedInfo.getCrossroadId());
                carOverSpeedLimitInfo.setCityId(carSpeedInfo.getCityId());
                carOverSpeedLimitInfo.setTs(System.currentTimeMillis());
                collector.collect(carOverSpeedLimitInfo);
                return;
            }
            log.info("地区：{}，当前限速：{}，当前汽车：{}，车速：{}，时间:{}", key, speedLimit, carSpeedInfo.getCarNo(), carSpeedInfo.getCurrentSpeed(), System.currentTimeMillis());
            return;
        }
        // 无限速规则
        log.info("地区：{}，当前限速：无，当前汽车：{}，车速：{}，时间:{}", key, carSpeedInfo.getCarNo(), carSpeedInfo.getCurrentSpeed(), System.currentTimeMillis());
    }

    // 对限速规则处理
    @Override
    public void processBroadcastElement(StreetSpeedLimitRule streetSpeedLimitRule, BroadcastProcessFunction<CarSpeedInfo, StreetSpeedLimitRule, CarOverSpeedLimitInfo>.Context context, Collector<CarOverSpeedLimitInfo> collector) throws Exception {
        log.info("streetSpeedLimitRule processBroadcastElement:{}", streetSpeedLimitRule);
        BroadcastState<String, StreetSpeedLimitRule> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // 把当前限速规则保存到 ctx 中
        String key = buildMapStateKey(streetSpeedLimitRule.getCityId(), streetSpeedLimitRule.getStreetId(), streetSpeedLimitRule.getCrossroadId());
        broadcastState.put(key, streetSpeedLimitRule);
        log.info("streetSpeedLimitRule success:{}", streetSpeedLimitRule);
    }
}
