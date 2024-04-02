package it.com.function;

import it.com.entity.CarSpeedInfo;
import it.com.entity.RepatitionCarInfo;
import it.com.util.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 套牌车辆检查
 */
@Slf4j
public class RepatitionCarKeyedProcessFunction extends KeyedProcessFunction<String, CarSpeedInfo, RepatitionCarInfo> {

    /**
     * 值状态，保存上次车辆路过的信息
     */
    private transient ValueState<Long> lastTimeState;

    @Override
    public void open(Configuration parameters) {
        // 状态描述器
        ValueStateDescriptor<Long> flagDescriptor = new ValueStateDescriptor<>("lastTime", Types.LONG);
        // 设置状态描述器
        lastTimeState = getRuntimeContext().getState(flagDescriptor);
    }

    @Override
    public void processElement(CarSpeedInfo carSpeedInfo, KeyedProcessFunction<String, CarSpeedInfo, RepatitionCarInfo>.Context context, Collector<RepatitionCarInfo> collector) throws Exception {
        // 当前元素的时间
        long currentTime = carSpeedInfo.getTs();
        // 直接拿状态中存储的值
        Long lastTime = lastTimeState.value();
        if (lastTime != null) {
            long dur = currentTime - lastTime;
            // 两次间隔时间 < 10s
            if (dur < 10 * 1000) {
                RepatitionCarInfo repatitionCarInfo = new RepatitionCarInfo();
                repatitionCarInfo.setCarNo(carSpeedInfo.getCarNo());
                repatitionCarInfo.setCrossroadId(carSpeedInfo.getCrossroadId());
                repatitionCarInfo.setStreetId(carSpeedInfo.getStreetId());
                repatitionCarInfo.setCityId(carSpeedInfo.getCityId());
                repatitionCarInfo.setThisTime(currentTime);
                repatitionCarInfo.setLastTime(lastTime);
                log.info("车辆{}可能涉嫌套牌，在{}卡扣多次出现，上次出现时间：{}，本次出现时间：{}，时间间隔：{}s", repatitionCarInfo.getCarNo(), repatitionCarInfo.getCrossroadId(), DateUtil.formatHHmmss(repatitionCarInfo.getLastTime()), DateUtil.formatHHmmss(repatitionCarInfo.getThisTime()), dur);
                collector.collect(repatitionCarInfo);
                return;
            }
            // 更新状态数据，需要对比一下，毕竟有乱序问题
            if (currentTime > lastTime) {
                lastTimeState.update(currentTime);
            }
        } else {
            //状态中不包含当前车辆
            lastTimeState.update(currentTime);
        }
    }
}
