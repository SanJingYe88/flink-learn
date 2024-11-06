package it.com.function.state;

import it.com.entity.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 测试 Keyed State，实现自定义的 FlatMapFunction
 */
public class UserStateFlatMapFunction extends RichFlatMapFunction<Event, String> {

    /**
     * 值状态
     */
    private ValueState<Event> eventValueState;
    /**
     * 列表状态
     */
    private ListState<Event> eventListState;
    /**
     * 键值状态
     */
    private MapState<String, Long> eventMapState;
    /**
     * 规约状态
     */
    private ReducingState<Event> eventReduceState;
    /**
     * 聚合状态
     */
    private AggregatingState<Event, String> eventAggregatingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取状态的运行上下文，方便调用状态等等。
        eventValueState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("my-value", Types.POJO(Event.class)));
        eventListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Types.POJO(Event.class)));
        eventMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));

        eventReduceState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("my-reduce",
                new ReduceFunction<Event>() {
                    @Override
                    public Event reduce(Event value1, Event value2) {
                        return new Event(value1.getName(), value1.getUrl(), value2.getTs());
                    }
                },
                Types.POJO(Event.class)));

        eventAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("my-flatmap",
                new AggregateFunction<Event, Long, String>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Event value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public String getResult(Long accumulator) {
                        return "count:" + accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                Long.class));
    }

    @Override
    public void flatMap(Event value, Collector<String> out) throws Exception {
        // 访问和更新状态
        System.out.println("原始状态:" + eventValueState.value());
        eventValueState.update(value);
        System.out.println("更新的状态:" + eventValueState.value());
    }

    @Override
    public void close() {
        // 清除状态
        eventValueState.clear();
    }
}