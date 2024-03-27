package it.com.function.ds;

import it.com.entity.User;
import it.com.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 可以生成乱序数据，自定义的 DataGeneratorSource，需要实现 SourceFunction 接口
 */
@Slf4j
public class UserDelayDataGeneratorSource implements SourceFunction<User> {

    public RandomDataGenerator generator = new RandomDataGenerator();
    private volatile int num;
    private volatile boolean running = true;

    public UserDelayDataGeneratorSource(int num) {
        this.num = num;
    }

    @Override
    public void run(SourceContext<User> sourceContext) throws Exception {
        List<User> list = new ArrayList<>();
        Random random = new Random();
        while (running) {
            User user = new User();
            user.setId(new Random().nextInt(100));
            String name = generator.nextHexString(4);
            user.setName(name);
            log.info("生成元素：{}", JsonUtils.toJson(user));
            // 模拟一些延迟，以便可以看到水位线的效果
            Thread.sleep(random.nextInt(1000) + 500);
            // 设置事件时间戳为当前时间（实际中应该是模拟的时间）
            user.setTs(System.currentTimeMillis());
            list.add(user);
            num--;
            if (num <= 0) {
                running = false;
            }
        }

        // 随机排序
        Collections.shuffle(list);
        // 循环发出事件
        list.forEach(sourceContext::collect);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
