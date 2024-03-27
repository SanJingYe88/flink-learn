package it.com.watermark;

import it.com.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.*;

/**
 * 自定义 标记性水位线生成策略
 */
@Slf4j
public class PeriodWatermarkStrategy implements WatermarkStrategy<User> {

    /**
     * 实例化一个 事件时间提取器
     *
     * @param context
     * @return
     */
    @Override
    public TimestampAssigner<User> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        TimestampAssigner<User> timestampAssigner = new TimestampAssigner<User>() {
            /**
             * 需要实现 TimestampAssigner 接口，实现该方法
             * @param element 流中的一个元素
             * @param recordTimestamp 这个值通常表示数据从外部系统（如 Kafka）进入 Flink 时的时间
             * @return
             */
            @Override
            public long extractTimestamp(User element, long recordTimestamp) {
                log.info("当前元素，提取事件时间:{}, recordTimestamp:{}", element.getTs(), recordTimestamp);
                // 这里使用事件生成的时间
                return element.getTs();
            }
        };
        return timestampAssigner;
    }

    // 实例化一个 watermark 生成器
    @Override
    public WatermarkGenerator<User> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new PeriodWatermarkGenerator<>();
    }
}