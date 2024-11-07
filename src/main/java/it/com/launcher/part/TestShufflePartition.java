package it.com.launcher.part;

import it.com.entity.Event;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 shuffle
 */
public class TestShufflePartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源,并行度为1
        DataStreamSource<Event> stream = DataGeneratorSourceUtil.fromElementsUserUrlViews(env);

        //全部打散，随机分配下游，经洗牌后打印输出，并行度为 4
        stream.shuffle().print().setParallelism(4);

        /**
         * 多次运行后，查看输出可以发现，是完全随机分配的，有些分区可能不会被分配
         */
        env.execute();
    }
}
