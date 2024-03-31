package it.com.util;

import it.com.entity.CarSpeedInfo;
import it.com.entity.SanGuoUser;
import it.com.entity.StreetSpeedLimitRule;
import it.com.function.binlog.BinLogGeneratorFunction;
import it.com.function.generator.CarSpeedInfoGeneratorFunction;
import it.com.function.generator.SanGuoUserGeneratorFunction;
import it.com.function.generator.StreetSpeedLimitRuleGeneratorFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

    // 汽车速度数据mock
    public static DataGeneratorSource<CarSpeedInfo> carSpeedInfoDataGeneratorSource(int num) {
        DataGeneratorSource<CarSpeedInfo> dataGeneratorSource = new DataGeneratorSource<CarSpeedInfo>(
                new CarSpeedInfoGeneratorFunction(),
                num,
                RateLimiterStrategy.perSecond(1),
                TypeInformation.of(CarSpeedInfo.class)
        );
        log.info("carSpeedInfoDataGeneratorSource start...");
        return dataGeneratorSource;
    }

    // 汽车限速规则数据mock
    public static DataGeneratorSource<StreetSpeedLimitRule> streetSpeedLimitRuleDataGeneratorSource(int num) {
        DataGeneratorSource<StreetSpeedLimitRule> dataGeneratorSource = new DataGeneratorSource<StreetSpeedLimitRule>(
                new StreetSpeedLimitRuleGeneratorFunction(),
                num,
                RateLimiterStrategy.perSecond(0.5),
                TypeInformation.of(StreetSpeedLimitRule.class)
        );
        log.info("streetSpeedLimitRuleDataGeneratorSource start...");
        return dataGeneratorSource;
    }

    /**
     * 从指定文件中读取数据
     *
     * @param filePath 指定文件
     * @return FileSource
     */
    public static FileSource<String> readFile(String filePath) {
        // import org.apache.flink.connector.file.src.FileSource;
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat()
                // 注意 Path 的包 import org.apache.flink.core.fs.Path;
                , new Path(filePath)).build();
        log.info("readFile,filePath;{}", filePath);
        return fileSource;
    }

    /**
     * 从 socket 流中获取数据
     * @param env
     * @param hostName
     * @param port
     * @return
     */
    public static DataStreamSource<String> fromSocket(StreamExecutionEnvironment env, String hostName, int port) {
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777);
        log.info("监控IP：{}, 端口：{}", hostName, port);
        return dataStreamSource;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(DataGeneratorSourceUtil.carSpeedInfoDataGeneratorSource(10), WatermarkStrategy.noWatermarks(), "data-generator").print();
        env.execute();
    }
}
