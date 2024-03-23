package it.com.util;

import it.com.entity.SanGuoUser;
import it.com.function.SanGuoUserGeneratorFunction;
import it.com.function.binlog.BinLogGeneratorFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;

/**
 * mock数据工具类
 */
@Slf4j
public class DataGeneratorSourceUtil {

    public static DataGeneratorSource<SanGuoUser> sanGuoUserDataGeneratorSource(int num) {
        DataGeneratorSource<SanGuoUser> dataGeneratorSource = new DataGeneratorSource<SanGuoUser>(
                new SanGuoUserGeneratorFunction(),
                num,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(SanGuoUser.class)
        );
        log.info("sanGuoUserDataGeneratorSource start...");
        return dataGeneratorSource;
    }

    public static DataGeneratorSource<Tuple2<String, String>> binlogDataGeneratorSource(int num) {
        DataGeneratorSource<Tuple2<String, String>> dataGeneratorSource = new DataGeneratorSource<Tuple2<String, String>>(
                // 指定生成数据的具体实现类
                new BinLogGeneratorFunction(),
                // 指定 输出数据的总行数
                num,
                // 指定 每秒发射的记录数
                RateLimiterStrategy.perSecond(100),
                // 指定返回类型
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                })
        );
        return dataGeneratorSource;
    }
}
