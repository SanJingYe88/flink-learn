package it.com.launcher.op;

import it.com.entity.Event;
import it.com.function.part.UserIdKeyedSelector;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 键控算子
 */
public class TestKeyedOperationLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 数据
        DataStreamSource<Event> dataStreamSource = DataGeneratorSourceUtil.fromElementsUserUrlViews(env);

        // keyBy 算子
        KeyedStream<Event, Integer> keyedStream = dataStreamSource.keyBy(new UserIdKeyedSelector());

        keyedStream.print("keyBy");

        // name分区，获取 ts max 的数据
        dataStreamSource.keyBy(Event::getName).max("ts").print("max");
        // name分区，获取 ts maxBy 的数据
        dataStreamSource.keyBy(Event::getName).maxBy("ts").printToErr("maxBy");

        /**
         * maxBy> Event(name=Mary, url=./home, ts=2000)
         * maxBy> Event(name=Bob, url=./cart, ts=2000)
         * maxBy> Event(name=Alice, url=./prod?id=100, ts=3000)
         * maxBy> Event(name=Bob, url=./prod?id=1, ts=3300)
         * maxBy> Event(name=Mary, url=./home, ts=2000)
         *
         * max> Event(name=Mary, url=./home, ts=2000)
         * max> Event(name=Bob, url=./cart, ts=2000)
         * max> Event(name=Alice, url=./prod?id=100, ts=3000)
         * max> Event(name=Bob, url=./cart, ts=3300)
         * max> Event(name=Mary, url=./home, ts=2000)
         */

        // 不分区，获取 ts max 的数据
        dataStreamSource.keyBy(key -> "key").max("ts").print("max");
        // 不分区，获取 ts maxBy 的数据
        dataStreamSource.keyBy(key -> "key").maxBy("ts").printToErr("maxBy");

        /**
         * max> Event(name=Mary, url=./home, ts=2000)
         * max> Event(name=Mary, url=./home, ts=2000)
         * max> Event(name=Mary, url=./home, ts=3000)
         * max> Event(name=Mary, url=./home, ts=3300)
         * max> Event(name=Mary, url=./home, ts=3300)
         *
         * maxBy> Event(name=Mary, url=./home, ts=2000)
         * maxBy> Event(name=Mary, url=./home, ts=2000)
         * maxBy> Event(name=Alice, url=./prod?id=100, ts=3000)
         * maxBy> Event(name=Bob, url=./prod?id=1, ts=3300)
         * maxBy> Event(name=Bob, url=./prod?id=1, ts=3300)
         */
        env.execute();

        env.execute();
    }
}
