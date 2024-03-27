package it.com.watermark;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 自定义 标记水位线生成器 实现类
 *
 * @param <T>
 */
@Slf4j
public class PeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    /*
     * 每进入一条数据，都会调用一次 onEvent 方法
     *   event ： 进入到该方法的事件数据
     *   eventTimestamp ： 时间戳提取器提取的时间戳
     * */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        log.info("发射水位线：{},eventTimestamp:{}", event, eventTimestamp);
        //发射水位线
        output.emitWatermark(new Watermark(eventTimestamp));
    }

    // 不需要实现
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
    }
}
