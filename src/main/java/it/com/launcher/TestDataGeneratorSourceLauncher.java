package it.com.launcher;

import it.com.function.binlog.BinLogGeneratorFunction;
import it.com.function.binlog.BinlogFormatFunction;
import it.com.function.common.EventFilterFunction;
import it.com.function.common.FlatDataMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从 数据生成器(DataGenerator) 中读取数据
 */
public class TestDataGeneratorSourceLauncher {
    public static void main(String[] args) throws Exception {
        // 1.获取env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.自定义数据生成器 Source
        // 注意包 import org.apache.flink.connector.datagen.source.DataGeneratorSource;
        DataGeneratorSource<Tuple2<String, String>> dataGeneratorSource = new DataGeneratorSource<Tuple2<String, String>>(
                // 指定生成数据的具体实现类
                new BinLogGeneratorFunction(),
                // 指定 输出数据的总行数
                100,
                // 指定 每秒发射的记录数
                RateLimiterStrategy.perSecond(100),
                // 指定返回类型
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                })
        );

        // 3.读取 dataGeneratorSource 中的数据
        env.fromSource(dataGeneratorSource  // 指定 ds
                        , WatermarkStrategy.noWatermarks()  // 指定水位线生成策略
                        , "data-generator")
                // 格式化
                .flatMap(new BinlogFormatFunction())
                // 打平
                .flatMap(new FlatDataMapFunction())
                // 去重
                .filter(new EventFilterFunction())
        ;
        env.execute();
    }
}
