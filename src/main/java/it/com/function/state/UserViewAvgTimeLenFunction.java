package it.com.function.state;


import it.com.entity.ViewTimeLenEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class UserViewAvgTimeLenFunction extends RichFlatMapFunction<ViewTimeLenEvent, String> {
    /**
     * 自定义的窗口大小
     */
    private Long count;

    public UserViewAvgTimeLenFunction(Long count) {
        this.count = count;
    }

    /**
     * 定义一个聚合状态，用来保存平均浏览时长
     */
    private AggregatingState<ViewTimeLenEvent, Long> avgTimeLenAggState;

    /**
     * 定义一个值状态，保存用户访问的次数
     */
    private ValueState<Long> viewCountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取运行上下文
        //ACC : Tuple2<Long,Long> : 第一个参数是所有时间戳的和，第二个参数是当前的个数
        //Out : 平均时间戳
        avgTimeLenAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<ViewTimeLenEvent, Tuple2<Long, Long>, Long>(
                "avg-ts",
                new AggregateFunction<ViewTimeLenEvent, Tuple2<Long, Long>, Long>() {
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L, 0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(ViewTimeLenEvent value, Tuple2<Long, Long> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.getViewTimeLen(), accumulator.f1 + 1);
                    }

                    @Override
                    public Long getResult(Tuple2<Long, Long> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return null;
                    }
                },
                Types.TUPLE(Types.LONG, Types.LONG)
        ));

        viewCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
    }

    @Override
    public void flatMap(ViewTimeLenEvent value, Collector<String> out) throws Exception {
        // 每来一条数据 count 就 +1
        Long currCount = viewCountState.value();

        // 判断，如果状态里面没有值的话，就+1，如果有值的话，就++
        if (currCount == null) {
            currCount = 1L;
        } else {
            currCount++;
        }

        // 更新状态
        viewCountState.update(currCount);
        avgTimeLenAggState.add(value);

        //根据 count,判断是否达到了累积的技术窗口的长度
        if (currCount.equals(count)) {
            out.collect(value.getName() + "过去" + count + "次访问平均浏览时长为：" + avgTimeLenAggState.get());
            // 清空状态
            avgTimeLenAggState.clear();
            viewCountState.clear();
        }
    }
}
