package it.com.launcher.bc;

import it.com.entity.SanGuoUser;
import it.com.function.bc.ActionBroadcastProcessFunction;
import it.com.util.DataGeneratorSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 广播连接流 BroadcastConnectedStream
 */
public class TestBroadcastConnectedStream2Launcher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 两个不同类型的数据源
        DataGeneratorSource<SanGuoUser> dataGeneratorSource1 = DataGeneratorSourceUtil.sanGuoUserDataGeneratorSource(10);
        DataGeneratorSource<Tuple2<String, String>> dataGeneratorSource2 = DataGeneratorSourceUtil.binlogDataGeneratorSource(10);

        // 两条不同类型的数据流
        // 事件流
        DataStreamSource<SanGuoUser> actionStreamSource = env.fromSource(dataGeneratorSource1, WatermarkStrategy.noWatermarks(), "data-generator-1");
        // 规则流
        DataStreamSource<Tuple2<String, String>> ruleStreamSource = env.fromSource(dataGeneratorSource2, WatermarkStrategy.noWatermarks(), "data-generator-2", TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        // 规则流转为广播流
        MapStateDescriptor<Integer, Tuple2<String, String>> mapStateDescriptor = new MapStateDescriptor<>("broadcast-state", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));
        BroadcastStream<Tuple2<String, String>> broadcastStream = ruleStreamSource.broadcast(mapStateDescriptor);
        // 合并流，事件流合并广播流
        BroadcastConnectedStream<SanGuoUser, Tuple2<String, String>> connectedStream = actionStreamSource.connect(broadcastStream);
        SingleOutputStreamOperator<SanGuoUser> processStream = connectedStream.process(new ActionBroadcastProcessFunction());
        processStream.print();

        env.execute();
    }
}

