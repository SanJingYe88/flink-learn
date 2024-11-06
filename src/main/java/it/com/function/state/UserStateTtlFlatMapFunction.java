package it.com.function.state;

import it.com.entity.Event;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 测试 State TTL，实现自定义的 FlatMapFunction
 */
public class UserStateTtlFlatMapFunction extends RichFlatMapFunction<Event, String> {

    /**
     * 定义状态
     */
    private ValueState<Event> eventValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取状态的运行上下文，方便调用状态等等。
        ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("my-value", Types.POJO(Event.class));
        eventValueState = getRuntimeContext().getState(valueStateDescriptor);

        // 配置状态的TTL
        StateTtlConfig tlConfig = StateTtlConfig.newBuilder(Time.seconds(10)) // 状态的生存时间
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 更新的类型，(给一个更新的初始值)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 这个选项配置是否允许返回已过期的用户值
                .build();
        /**
         * 如果过期的用户值尚未被清理，则返回该值。ReturnExpiredIfNotCleanedUp
         * 永远不返回过期的用户值。NeverReturnExpired
         */

        valueStateDescriptor.enableTimeToLive(tlConfig);
    }

    @Override
    public void flatMap(Event value, Collector<String> out) throws Exception {

    }
}