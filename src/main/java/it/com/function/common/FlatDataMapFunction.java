package it.com.function.common;


import it.com.util.NestedMapFlattenerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * json 格式打平
 */
@Slf4j
public class FlatDataMapFunction extends RichFlatMapFunction<Tuple2<String, String>, Map<String, Object>> {

    @Override
    public void open(Configuration parameters) {
        log.info("FlatDataMapFunction task start");
    }

    @Override
    public void flatMap(Tuple2<String, String> topicAndValue, Collector<Map<String, Object>> out) {
        String json = topicAndValue.f1;
        try {
            // 展开
            Map<String, Object> flatDataMap = NestedMapFlattenerUtils.flattenJson(json);
            if (MapUtils.isEmpty(flatDataMap)) {
                return;
            }
            out.collect(flatDataMap);
        } catch (Exception e) {
            log.info("FlatDataMapFunction task error :", e);
        }
    }
}



