package it.com.launcher;

import it.com.util.DataGeneratorSourceUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试，从 socket 流中获取数据
 */
@Slf4j
public class TestSocketLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = DataGeneratorSourceUtil.fromSocket(env, "localhost", 7777);
        dataStreamSource.print();
        env.execute();
    }
}
