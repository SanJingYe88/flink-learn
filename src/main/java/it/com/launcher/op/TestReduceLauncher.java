package it.com.launcher.op;

import it.com.entity.Event;
import it.com.function.ds.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试归约聚合 reduce
 */
public class TestReduceLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        //当前访问量最大，也就是点击次数最多的，最活跃的用户是谁
        //1.统计每个用户的访问频次
        KeyedStream<Tuple2<String, Long>, String> keyedStream = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.getName(), 1L);
            }
        }).keyBy(data -> data.f0);

        // 归约，统计次数
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = keyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        //2.选取当前最活跃的用户 ==> (keyBy之后,可能有分区操作,将所有的分区合并成一个分区,再次计算)
        clickByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        if (value1.f1 > value2.f1) { //如果老数据大于新数据,则返回老数据,否则返回新数据
                            return value1;
                        } else {
                            return value2;
                        }
                    }
                }).map(new MapFunction<Tuple2<String, Long>, Object>() {
                    @Override
                    public Object map(Tuple2<String, Long> value) throws Exception {
                        return "当前时间" + System.currentTimeMillis() + ",访问次数最多的用户【" + value.f0 + "】,访问次数:" + value.f1;
                    }
                }).print("分区编号");

        env.execute();
    }
}

