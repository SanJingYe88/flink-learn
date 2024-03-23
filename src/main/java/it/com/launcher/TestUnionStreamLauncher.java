package it.com.launcher;

import it.com.entity.SanGuoUser;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试合并流 union
 */
public class TestUnionStreamLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 自定义数据生成器Source
        DataGeneratorSource<SanGuoUser> dataGeneratorSource1 = DataGeneratorSourceUtil.sanGuoUserDataGeneratorSource(1);

        // 两条数据流
        DataStreamSource<SanGuoUser> streamSource1 = env.fromSource(dataGeneratorSource1, WatermarkStrategy.noWatermarks(), "data-generator-1");
        DataStreamSource<SanGuoUser> streamSource2 = env.fromSource(dataGeneratorSource1, WatermarkStrategy.noWatermarks(), "data-generator-2");

        // union合并流，数据类型必须相同才能合并
        DataStream<SanGuoUser> dataStream = streamSource1.union(streamSource2);
        dataStream.print();
        env.execute();
    }
}