package it.com.launcher.op;


import it.com.entity.Event;
import it.com.function.ds.ClickSource;
import it.com.function.op.UserRichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 UDF 函数
 */
public class TestRichMapFunctionLauncher {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.map(new UserRichMapFunction()).print();
        env.execute();
    }
}

