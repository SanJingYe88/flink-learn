package it.com.launcher.op;

import it.com.entity.Event;
import it.com.entity.ViewTimeLenEvent;
import it.com.function.op.Event2ViewTimeLenEventMapFunction;
import it.com.function.op.EventFilterFunction;
import it.com.function.op.EventFlatMapFunction;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 基本转换算子
 */
public class TestBaseOperationLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 数据
        DataStreamSource<Event> dataStreamSource = DataGeneratorSourceUtil.fromElementsUserUrlViews(env);

        // map 算子
        SingleOutputStreamOperator<ViewTimeLenEvent> map1 = dataStreamSource.map(new Event2ViewTimeLenEventMapFunction());

        // map算子使用匿名函数
        SingleOutputStreamOperator<String> map2 = dataStreamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.getName();
            }
        });

        // map 算子使用 lambda
        SingleOutputStreamOperator<String> map3 = dataStreamSource.map((MapFunction<Event, String>) Event::getName);

        // filter 算子
        SingleOutputStreamOperator<Event> filter1 = dataStreamSource.filter(new EventFilterFunction());

        // flatMap 算子
        SingleOutputStreamOperator<ViewTimeLenEvent> flatMap = dataStreamSource.flatMap(new EventFlatMapFunction());

        map1.print("map1");
        map2.print("map2");
        map3.print("map3");
        filter1.print("filter1");
        flatMap.print("flatMap");

        env.execute();
    }
}
