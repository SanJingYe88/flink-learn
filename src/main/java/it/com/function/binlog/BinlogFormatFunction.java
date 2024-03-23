package it.com.function.binlog;

import it.com.entity.BinlogDto;
import it.com.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@Slf4j
public class BinlogFormatFunction extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {
    @Override
    public void open(Configuration parameters) {
        log.info("BinlogFormatFunction task start");
    }

    @Override
    public void flatMap(Tuple2<String, String> topicAndValue, Collector<Tuple2<String, String>> out) {
        log.info("BinlogFormatFunction :{}", JsonUtils.toJson(topicAndValue));
        try {
            String eventString = topicAndValue.f1;
            BinlogDto binlogDto = JsonUtils.toBean(eventString, BinlogDto.class);
            if (binlogDto == null) {
                return;
            }
            log.info("binlogDto: {}", binlogDto);

            String json = JsonUtils.toJson(binlogDto);
            out.collect(new Tuple2<>(topicAndValue.f0, json));
        } catch (Exception e) {
            log.info("BinlogFormatFunction task error :", e);
        }
    }
}


