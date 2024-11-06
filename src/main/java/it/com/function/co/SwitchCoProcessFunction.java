package it.com.function.co;

import it.com.entity.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class SwitchCoProcessFunction extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

    private ValueState<Boolean> forwardingEnabled;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN));
    }

    @Override
    public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
        if (forwardingEnabled.value() != null && forwardingEnabled.value()) {
            out.collect(value);
        }
    }

    @Override
    public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
        forwardingEnabled.update(true);
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        forwardingEnabled.clear();
    }
}