package it.com.function.op;

import it.com.entity.Event;
import it.com.entity.ViewTimeLenEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * flatMap 算子是 filter 和 map 的泛化。所以flatMap可以实现map和filter算子的功能。对每一个输入事件flatMap可以生成0个、1个或者多个输出元素.
 * 需要实现 FlatMapFunction<T,O> 接口，T:输出元素的类型，O：输出元素的类型
 */
public class EventFlatMapFunction implements FlatMapFunction<Event, ViewTimeLenEvent> {

    @Override
    public void flatMap(Event value, Collector<ViewTimeLenEvent> out) throws Exception {
        String name = value.getName();
        if(name.length() > 3){
            return;
        }
        ViewTimeLenEvent viewTimeLenEvent = new ViewTimeLenEvent();
        viewTimeLenEvent.setId(value.getId());
        viewTimeLenEvent.setName(value.getName());
        viewTimeLenEvent.setUrl(value.getUrl());
        viewTimeLenEvent.setViewTimeLen(100);
        viewTimeLenEvent.setTs(value.getTs());
        out.collect(viewTimeLenEvent);

        viewTimeLenEvent.setViewTimeLen(1001);
        out.collect(viewTimeLenEvent);
    }
}