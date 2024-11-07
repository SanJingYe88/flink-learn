package it.com.launcher.part;

import it.com.entity.Event;
import it.com.function.part.CustomKeySelector;
import it.com.function.part.CustomPartition;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 自定义分区
 */
public class TestCustomPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 数据
        DataStreamSource<Event> dataStreamSource = DataGeneratorSourceUtil.fromElementsUserUrlViews(env);

        // 自定义的分区规则
        CustomPartition customPartition = new CustomPartition();
        CustomKeySelector customKeySelector = new CustomKeySelector();

        // 将自然数按照奇偶分区
        DataStream<Event> eventDataStream = dataStreamSource.partitionCustom(customPartition, customKeySelector);

        eventDataStream.print("分区编号").setParallelism(4);

        env.execute();
    }
}

