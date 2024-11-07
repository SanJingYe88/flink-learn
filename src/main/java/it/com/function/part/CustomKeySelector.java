package it.com.function.part;

import it.com.entity.Event;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 自定义 key 提取器，根据输入的数据，获取数据分区的 key，即根据哪个字段分区
 */
public class CustomKeySelector implements KeySelector<Event,Integer> {

    @Override
    public Integer getKey(Event value) throws Exception {
        // 根据 id 分区
        return value.getId();
    }
}
