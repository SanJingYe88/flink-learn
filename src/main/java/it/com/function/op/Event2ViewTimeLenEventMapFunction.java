package it.com.function.op;

import it.com.entity.Event;
import it.com.entity.ViewTimeLenEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * map function 可以对元素进行转换
 * 需要实现 MapFunction<T,O> 接口，T:输出元素的类型，O：输出元素的类型
 */
public class Event2ViewTimeLenEventMapFunction implements MapFunction<Event, ViewTimeLenEvent> {
    @Override
    public ViewTimeLenEvent map(Event value) throws Exception {
        ViewTimeLenEvent viewTimeLenEvent = new ViewTimeLenEvent();
        viewTimeLenEvent.setId(value.getId());
        viewTimeLenEvent.setName(value.getName());
        viewTimeLenEvent.setUrl(value.getUrl());
        viewTimeLenEvent.setViewTimeLen(100);
        viewTimeLenEvent.setTs(value.getTs());
        return viewTimeLenEvent;
    }
}
