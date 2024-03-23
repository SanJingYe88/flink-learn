package it.com.function.generator;

import it.com.entity.StreetSpeedLimitRule;
import it.com.util.RandomUtil;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

/**
 * mock数据生成 function
 */
public class StreetSpeedLimitRuleGeneratorFunction implements GeneratorFunction<Long, StreetSpeedLimitRule> {
    public RandomDataGenerator generator;

    @Override
    public void open(SourceReaderContext readerContext) {
        generator = new RandomDataGenerator();
    }

    @Override
    public StreetSpeedLimitRule map(Long key) {
        StreetSpeedLimitRule streetSpeedLimitRule = new StreetSpeedLimitRule();
        streetSpeedLimitRule.setId(String.valueOf(key));
        streetSpeedLimitRule.setCityId(1);
        streetSpeedLimitRule.setStreetId(1);
        int i = new Random().nextInt(3);
        streetSpeedLimitRule.setCrossroadId(i);
        streetSpeedLimitRule.setLimitSpeed(RandomUtil.generateRandomDoubleBetween60And120());
        streetSpeedLimitRule.setUpdateTime(System.currentTimeMillis());
        return streetSpeedLimitRule;
    }
}
