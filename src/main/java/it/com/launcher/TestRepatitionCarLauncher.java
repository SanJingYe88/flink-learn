package it.com.launcher;

import it.com.entity.CarSpeedInfo;
import it.com.entity.RepatitionCarInfo;
import it.com.function.RepatitionCarKeyedProcessFunction;
import it.com.util.DataGeneratorSourceUtil;
import it.com.util.JsonUtils;
import it.com.util.RandomUtil;
import it.com.util.SinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

/**
 * 实时监测可能套牌的车辆信息
 * 值状态编程
 */
@Slf4j
public class TestRepatitionCarLauncher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = DataGeneratorSourceUtil.fromSocket(env, "localhost", 7777);
        // nc -lp 7777 打开 7777 端口，然后输入 车牌号就可触发数据生成
        SingleOutputStreamOperator<CarSpeedInfo> outputStreamOperator = dataStreamSource.map((MapFunction<String, CarSpeedInfo>) s -> {
            CarSpeedInfo carSpeedInfo = new CarSpeedInfo();
            carSpeedInfo.setId(UUID.randomUUID().toString());
            carSpeedInfo.setCityId(1);
            carSpeedInfo.setStreetId(1);
            String carNo = "京A" + s;
            carSpeedInfo.setCarNo(carNo);
            carSpeedInfo.setCurrentSpeed(RandomUtil.generateRandomDoubleBetween60And120());
            carSpeedInfo.setTs(System.currentTimeMillis());
            return carSpeedInfo;
        });

        outputStreamOperator
                // 按键分组，按照车牌号分组
                .keyBy(CarSpeedInfo::getCarNo)
                // 套牌车辆检查
                .process(new RepatitionCarKeyedProcessFunction())
                // 输出为 json 字符串
                .flatMap((FlatMapFunction<RepatitionCarInfo, String>) (repatitionCarInfo, collector) -> collector.collect(JsonUtils.toJson(repatitionCarInfo)))
                .returns(Types.STRING)
                .print();
//                .sinkTo(SinkUtil.sinkToFile());
        env.execute();
    }
}
