package it.com.function.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * 去重
 */
@Slf4j
public class EventFilterFunction extends RichFilterFunction<Map<String, Object>> {

    @Override
    public void open(Configuration parameters) {
        log.info("EventConditionFilterFunction task start");
        // 打开 redis
    }

    @Override
    public boolean filter(Map<String, Object> flatDataMap) {
        try {
            Boolean isFilter = mockFilter();
            if (isFilter) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            log.info("EventConditionFilterFunction task error :", e);
            return false;
        }
    }


    /**
     * 模拟从 redis 中进行数据去重
     *
     * @return
     */
    private boolean mockFilter() {
        return true;
    }
}


