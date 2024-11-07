package it.com.launcher.part;

import it.com.entity.Event;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 rescale
 */
public class TestRebalancePartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源,并行度为1
        DataStreamSource<Event> stream = DataGeneratorSourceUtil.fromElementsUserUrlViews(env);

        // 经轮询重分区后打印输出，并行度为 4
        stream.rescale().print("rebalance").setParallelism(4);

        /**
         * rebalance:1> Event(name=Alice, url=./prod?id=100, ts=3000)
         * rebalance:4> Event(name=Bob, url=./cart, ts=2000)
         * rebalance:3> Event(name=Mary, url=./home, ts=1000)
         * rebalance:2> Event(name=Bob, url=./prod?id=1, ts=3300)
         * rebalance:4> Event(name=Alice, url=./prod?id=200, ts=3200)
         * rebalance:1> Event(name=Bob, url=./prod?id=2, ts=3800)
         * rebalance:3> Event(name=Bob, url=./home, ts=3500)
         * rebalance:2> Event(name=Bob, url=./prod?id=3, ts=4200)
         */
        env.execute();
    }
}