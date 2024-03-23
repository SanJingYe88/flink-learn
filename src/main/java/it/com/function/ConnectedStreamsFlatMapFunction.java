package it.com.function;

import it.com.entity.SanGuoUser;
import it.com.util.JsonUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 同处理方法
 * <p>
 * 需要实现 CoFlatMapFunction 接口，参数1：流1的类型，参数2：流2的类型，参数3：输出的类型
 */
public class ConnectedStreamsFlatMapFunction implements CoFlatMapFunction<SanGuoUser, Tuple2<String, String>, String> {

    @Override
    public void flatMap1(SanGuoUser value, Collector<String> out) throws Exception {
        out.collect(JsonUtils.toJson(value));
    }

    @Override
    public void flatMap2(Tuple2<String, String> value, Collector<String> out) throws Exception {
        out.collect(value.f1);
    }
}

