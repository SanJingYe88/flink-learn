package it.com.launcher.cep;

import it.com.entity.LoginEvent;
import it.com.util.DataGeneratorSourceUtil;
import it.com.util.JsonUtils;
import it.com.util.RandomUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * 测试 CEP 使用
 */
@Slf4j
public class TestLoginFailDetectLauncher {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // nc -lp 7777 打开 7777 端口，然后输入 车牌号就可触发数据生成
        DataStreamSource<String> dataStreamSource = DataGeneratorSourceUtil.fromSocket(env, "localhost", 7777);
        SingleOutputStreamOperator<LoginEvent> outputStreamOperator = dataStreamSource.map((MapFunction<String, LoginEvent>) s -> {
            LoginEvent loginEvent = new LoginEvent();
            loginEvent.setUserId(s);
            loginEvent.setIpAddress("x");
            loginEvent.setEventType(RandomUtil.generateLoginEventType());
            loginEvent.setTimestamp(System.currentTimeMillis());
            log.info("loginEvent:{}", JsonUtils.toJson(loginEvent));
            return loginEvent;
        });
        // 数据流
        KeyedStream<LoginEvent, String> loginEventStringKeyedStream = outputStreamOperator.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps().withTimestampAssigner(
                (SerializableTimestampAssigner<LoginEvent>) (loginEvent, l) -> loginEvent.getTimestamp()
        )).keyBy(LoginEvent::getUserId);

        // 设置CEP规则
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first-fail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) {
                return "fail".equals(loginEvent.getEventType());
            }
        }).next("second-fail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) {
                return "fail".equals(loginEvent.getEventType());
            }
        }).next("third-fail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent) {
                return "fail".equals(loginEvent.getEventType());
            }
        });

        // CEP 规则匹配
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStringKeyedStream, pattern);

        // 将监测到的数据提取出来
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
                                 // 提取三次都登陆事件
                                 @Override
                                 public String select(Map<String, List<LoginEvent>> map) throws Exception {
                                     LoginEvent first = map.get("first-fail").get(0);
                                     LoginEvent second = map.get("second-fail").get(0);
                                     LoginEvent third = map.get("third-fail").get(0);
                                     return first.getUserId() + "连续三次登录失败！登录时间：" + first.getTimestamp() + "," + second.getTimestamp() + "," + third.getTimestamp();
                                 }
                             }
        ).print();

        env.execute();
    }
}
