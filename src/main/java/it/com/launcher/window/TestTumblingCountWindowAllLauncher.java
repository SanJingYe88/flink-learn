package it.com.launcher.window;

import it.com.entity.ViewTimeLenEvent;
import it.com.function.ds.ViewTimeLenEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 测试 滚动 计数 窗口, 无 keyed 分区
 */
public class TestTumblingCountWindowAllLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 处理时间语义，不需要分配时间戳和 watermark
        SingleOutputStreamOperator<ViewTimeLenEvent> stream = env.addSource(new ViewTimeLenEventSource());

        // 每 10 个元素，求一次最长浏览时间，不分区用户
        AllWindowedStream<ViewTimeLenEvent, GlobalWindow> allWindowedStream = stream.countWindowAll(10);
        SingleOutputStreamOperator<String> singleOutputStreamOperatorAll = allWindowedStream.max("viewTimeLen")
                .map((MapFunction<ViewTimeLenEvent, String>) ViewTimeLenEvent::getUrl);

        stream.print("生产元素：");
        singleOutputStreamOperatorAll.print("最长浏览：");

        env.execute();
    }
}
