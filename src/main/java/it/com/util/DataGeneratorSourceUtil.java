package it.com.util;

import it.com.entity.SanGuoUser;
import it.com.function.SanGuoUserGeneratorFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;

@Slf4j
public class DataGeneratorSourceUtil {

    public static DataGeneratorSource<SanGuoUser> sanGuoUserDataGeneratorSource(int num){
        DataGeneratorSource<SanGuoUser> dataGeneratorSource = new DataGeneratorSource<SanGuoUser>(
                new SanGuoUserGeneratorFunction(),
                num,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(SanGuoUser.class)
        );
        log.info("sanGuoUserDataGeneratorSource start...");
        return dataGeneratorSource;
    }
}
