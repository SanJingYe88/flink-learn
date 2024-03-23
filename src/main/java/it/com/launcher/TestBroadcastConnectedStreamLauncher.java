package it.com.launcher;


import it.com.entity.CarOverSpeedLimitInfo;
import it.com.entity.CarSpeedInfo;
import it.com.entity.StreetSpeedLimitRule;
import it.com.function.StreetSpeedLimitRuleBroadcastProcessFunction;
import it.com.function.filter.CarOverSpeedFilterFunction;
import it.com.function.flatmap.CarOverSpeedFlatMapFunction;
import it.com.util.DataGeneratorSourceUtil;
import it.com.util.SinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试 广播连接流 BroadcastConnectedStream
 */
public class TestBroadcastConnectedStreamLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 汽车速度信息
        DataGeneratorSource<CarSpeedInfo> carSpeedInfoDataGeneratorSource = DataGeneratorSourceUtil.carSpeedInfoDataGeneratorSource(30);
        // 汽车规则信息
        DataGeneratorSource<StreetSpeedLimitRule> streetSpeedLimitRuleDataGeneratorSource = DataGeneratorSourceUtil.streetSpeedLimitRuleDataGeneratorSource(5);
        // 两条数据流
        DataStreamSource<CarSpeedInfo> carSpeedInfoDataStreamSource = env.fromSource(carSpeedInfoDataGeneratorSource, WatermarkStrategy.noWatermarks(), "carSpeedInfo-generator");
        DataStreamSource<StreetSpeedLimitRule> streetSpeedLimitRuleDataStreamSource = env.fromSource(streetSpeedLimitRuleDataGeneratorSource, WatermarkStrategy.noWatermarks(), "streetSpeedLimitRule-generator");

        // 设置规则流为广播流
        MapStateDescriptor<String, StreetSpeedLimitRule> mapStateDescriptor = new MapStateDescriptor<>(
                "broadcast-state",
                TypeInformation.of(String.class),
                TypeInformation.of(StreetSpeedLimitRule.class)
        );
        BroadcastStream<StreetSpeedLimitRule> streetSpeedLimitRuleBroadcastStream = streetSpeedLimitRuleDataStreamSource.broadcast(mapStateDescriptor);

        // 汽车速度信息流 连接  限速广播流
        SingleOutputStreamOperator<CarOverSpeedLimitInfo> process = carSpeedInfoDataStreamSource.connect(streetSpeedLimitRuleBroadcastStream)
                .process(new StreetSpeedLimitRuleBroadcastProcessFunction(mapStateDescriptor));

        streetSpeedLimitRuleDataStreamSource.printToErr("限速规则配置流>>>");
        // 过滤出超速车辆
        process.filter(new CarOverSpeedFilterFunction())
                // 转为 json
                .flatMap(new CarOverSpeedFlatMapFunction())
                // 输出到文件
                .sinkTo(SinkUtil.sinkToFile());

        env.execute();
    }
}
