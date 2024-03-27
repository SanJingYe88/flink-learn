package it.com.watermark;

import it.com.util.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * 自定义 周期性水位线生成器
 *
 * @param <User>
 */
@Slf4j
public class PunctuatedWatermarkGenerator<User> implements WatermarkGenerator<User> {
    // 保存当前最大的事件时间
    private long currentMaxTimestamp;
    // 指定最大的乱序时间(等待时间)
    private final long maxOutOfOrderness = 3000; // 3 秒

    @Override
    public void onEvent(User event, long eventTimestamp, WatermarkOutput output) {
        log.info("当前元素：{},eventTimestamp:{},currentMaxTimestamp:{}", event, DateUtil.formatHHmmss(eventTimestamp), DateUtil.formatHHmmss(currentMaxTimestamp));
        // 只更新当前最大时间戳，不再发射水位线
        if (currentMaxTimestamp < eventTimestamp) {
            currentMaxTimestamp = eventTimestamp;
            log.info("更新currentMaxTimestamp，currentMaxTimestamp:{}, System.currentTimeMillis:{}", DateUtil.formatHHmmss(currentMaxTimestamp), DateUtil.formatHHmmss(System.currentTimeMillis()));
        }
    }

    // 周期性 生成水位线, 通过 env.getConfig().setAutoWatermarkInterval 配置的时间间隔
    // 每间隔 setAutoWatermarkInterval 时间，调用一次该方法
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        log.info("周期性生成水位线,currentMaxTimestamp:{},maxOutOfOrderness={},System.currentTimeMillis:{}", DateUtil.formatHHmmss(currentMaxTimestamp), maxOutOfOrderness, DateUtil.formatHHmmss(System.currentTimeMillis()));
        // 发出的 watermark = 当前最大时间戳 - 最大乱序时间
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));
    }
}
