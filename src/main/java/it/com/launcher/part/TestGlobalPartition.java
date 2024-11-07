package it.com.launcher.part;

import it.com.entity.Event;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 全局分区
 */
public class TestGlobalPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = DataGeneratorSourceUtil.fromElementsUserUrlViews(env);

        stream.global().print().setParallelism(4);

        env.execute();
    }
}
