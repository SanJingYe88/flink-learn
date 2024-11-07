package it.com.function.part;

import it.com.entity.Event;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 测试按键分区算子
 * 需要实现 KeySelector<T,O> 接口，T: 当前流中的元素类型，O: key 的类型。
 */
public class UserIdKeyedSelector implements KeySelector<Event,Integer> {
    @Override
    public Integer getKey(Event value) throws Exception {
        // 按照 id 分区
        return value.getId();
    }
}
