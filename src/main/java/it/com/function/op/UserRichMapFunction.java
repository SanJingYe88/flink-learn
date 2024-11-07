package it.com.function.op;

import it.com.entity.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 测试 UDF 函数
 */
public class UserRichMapFunction extends RichMapFunction<Event,Integer> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //TODO getIndexOfThisSubtask 当前任务的索引
        System.out.println("open生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
    }

    @Override
    public Integer map(Event value) throws Exception {
        return value.getName().length();
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
    }
}
