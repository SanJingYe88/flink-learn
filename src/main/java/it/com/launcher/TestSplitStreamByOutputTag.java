package it.com.launcher;

import it.com.entity.SanGuoUser;
import it.com.function.CountryProcessFunction;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 侧输出流
 */
public class TestSplitStreamByOutputTag {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自定义数据生成器Source
        DataGeneratorSource<SanGuoUser> dataGeneratorSource = DataGeneratorSourceUtil.sanGuoUserDataGeneratorSource(10);

        // 分流后得到侧输出流
        SingleOutputStreamOperator<SanGuoUser> processedStream = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator")
                // 分流
                .process(new CountryProcessFunction());
        // 从侧输出流获取指定 TAG 流的数据
        processedStream.getSideOutput(CountryProcessFunction.WeiTag).print(CountryProcessFunction.WeiTag.getId());
        processedStream.getSideOutput(CountryProcessFunction.ShuTag).print(CountryProcessFunction.ShuTag.getId());
        processedStream.getSideOutput(CountryProcessFunction.WuTag).print(CountryProcessFunction.WuTag.getId());
        env.execute();
    }
}
