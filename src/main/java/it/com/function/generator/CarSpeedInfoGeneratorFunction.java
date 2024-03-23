package it.com.function.generator;

import it.com.entity.CarSpeedInfo;
import it.com.util.RandomUtil;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

/**
 * mock数据生成 function
 */
public class CarSpeedInfoGeneratorFunction implements GeneratorFunction<Long, CarSpeedInfo> {
    public RandomDataGenerator generator;

    @Override
    public void open(SourceReaderContext readerContext) {
        generator = new RandomDataGenerator();
    }

    @Override
    public CarSpeedInfo map(Long key) {
        CarSpeedInfo carSpeedInfo = new CarSpeedInfo();
        carSpeedInfo.setId(String.valueOf(key));
        carSpeedInfo.setCityId(1);
        carSpeedInfo.setStreetId(1);
        int i = new Random().nextInt(3);
        carSpeedInfo.setCrossroadId(i);
        String carNo = "京A" + generator.nextHexString(6);
        carSpeedInfo.setCarNo(carNo);
        carSpeedInfo.setCurrentSpeed(RandomUtil.generateRandomDoubleBetween60And120());
        carSpeedInfo.setTs(System.currentTimeMillis());
        return carSpeedInfo;
    }
}
