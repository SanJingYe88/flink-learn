package it.com.launcher.window;

import it.com.entity.ViewTimeLenEvent;
import it.com.function.ds.ViewTimeLenEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 测试 滚动 计数 窗口, 有 keyed 分区
 */
public class TestTumblingCountWindowLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 处理时间语义，不需要分配时间戳和 watermark
        SingleOutputStreamOperator<ViewTimeLenEvent> stream = env.addSource(new ViewTimeLenEventSource());

        WindowedStream<ViewTimeLenEvent, Integer, GlobalWindow> windowedStream = stream.keyBy(ViewTimeLenEvent::getId).countWindow(5);

        SingleOutputStreamOperator<Tuple2<String, String>> singleOutputStreamOperator = windowedStream.maxBy("viewTimeLen")
                .map(((MapFunction<ViewTimeLenEvent, Tuple2<String, String>>) value -> Tuple2.of(value.getName(), value.getUrl())))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        stream.print("生产出元素：");
        singleOutputStreamOperator.print("最长浏览：");

        env.execute();
    }
}
