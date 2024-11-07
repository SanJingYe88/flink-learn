package it.com.launcher.part;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 测试 rescale
 */
public class TestRescalePartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //缩放重分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i <= 8; i++) {
                            //将奇偶数分别发到0号和1号并行分区
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                }).setParallelism(2)
                .rescale()
                .print().setParallelism(4);

        env.execute();
    }
}
