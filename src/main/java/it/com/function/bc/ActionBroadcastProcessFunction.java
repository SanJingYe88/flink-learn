package it.com.function.bc;

import it.com.entity.SanGuoUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public  class ActionBroadcastProcessFunction extends BroadcastProcessFunction<SanGuoUser, Tuple2<String, String>, SanGuoUser> {

    /**
     * 正常处理数据,每当 主体基本流新增一条记录，该方法就会执行一次
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(SanGuoUser value, ReadOnlyContext ctx, Collector<SanGuoUser> out) throws Exception {
        log.info("value:{}",value);
        log.info("ctx:{}",ctx);
        out.collect(value);
    }

    /**
     * 用新规则来更新广播状态,每当 广播流新增一条记录，该方法就会执行一次
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<SanGuoUser> out) throws Exception {
        log.info("value:{}",value);
        log.info("ctx:{}",ctx);
    }
}

