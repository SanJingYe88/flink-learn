package it.com.launcher;


import it.com.entity.SanGuoUser;
import it.com.function.ConnectedStreamsFlatMapFunction;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试流连接并 key by
 */
public class TestConnectedStreamsKeyByLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 自定义数据生成器Source
        DataGeneratorSource<SanGuoUser> dataGeneratorSource1 = DataGeneratorSourceUtil.sanGuoUserDataGeneratorSource(10);
        DataGeneratorSource<Tuple2<String, String>> dataGeneratorSource2 = DataGeneratorSourceUtil.binlogDataGeneratorSource(10);

        // 两条不同类型的数据流
        DataStreamSource<SanGuoUser> streamSource1 = env.fromSource(dataGeneratorSource1, WatermarkStrategy.noWatermarks(), "data-generator-1");
        DataStreamSource<Tuple2<String, String>> streamSource2 = env.fromSource(dataGeneratorSource2, WatermarkStrategy.noWatermarks(), "data-generator-2", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));

        // connect合并流，只能合并两个流
        ConnectedStreams<SanGuoUser, Tuple2<String, String>> connectedStreams = streamSource1.connect(streamSource2);
        connectedStreams
                // 指定 key by 规则
                .keyBy(SanGuoUser::getCountry, data -> data.f1)
                .flatMap(new ConnectedStreamsFlatMapFunction())
                .print();
        ;
        env.execute();

    }
}

