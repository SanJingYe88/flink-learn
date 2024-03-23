package it.com.function.generator;

import it.com.entity.SanGuoUser;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * mock数据生成 function
 */
public class SanGuoUserGeneratorFunction implements GeneratorFunction<Long, SanGuoUser> {

    public RandomDataGenerator generator;

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        generator = new RandomDataGenerator();
    }

    @Override
    public SanGuoUser map(Long key) throws Exception {
        SanGuoUser sanGuoUser = new SanGuoUser();
        sanGuoUser.setId(key.intValue());
        String name = generator.nextHexString(4);
        sanGuoUser.setName(name);
        if (sanGuoUser.getId() % 3 == 0) {
            sanGuoUser.setCountry("魏国");
        } else if (sanGuoUser.getId() % 3 == 1) {
            sanGuoUser.setCountry("蜀国");
        } else {
            sanGuoUser.setCountry("吴国");
        }
        return sanGuoUser;
    }
}
