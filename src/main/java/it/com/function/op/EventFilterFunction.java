package it.com.function.op;

import it.com.entity.Event;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * filter function 可以对元素进行过滤，不满足条件的元素，将被丢弃
 * 需要实现 FilterFunction<T> 接口，T:输出元素的类型
 */
public class EventFilterFunction implements FilterFunction<Event> {

    @Override
    public boolean filter(Event value) throws Exception {
        return "sz".equals(value.getName());
    }
}
