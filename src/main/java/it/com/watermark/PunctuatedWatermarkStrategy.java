package it.com.watermark;

import it.com.entity.User;
import org.apache.flink.api.common.eventtime.*;

/**
 * 自定义 周期性水位线生成策略
 */
public class PunctuatedWatermarkStrategy implements WatermarkStrategy<User> {
    //  实例化一个 事件时间提取器
    @Override
    public TimestampAssigner<User> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        TimestampAssigner<User> timestampAssigner = new TimestampAssigner<User>() {
            @Override
            public long extractTimestamp(User element, long recordTimestamp) {
                return element.getTs();
            }
        };
        return timestampAssigner;
    }

    //  实例化一个 watermark 生成器
    @Override
    public WatermarkGenerator<User> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new PunctuatedWatermarkGenerator<>();
    }
}
