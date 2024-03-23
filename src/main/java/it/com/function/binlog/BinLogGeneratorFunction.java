package it.com.function.binlog;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * 生成随机数据
 */
@Slf4j
public class BinLogGeneratorFunction implements GeneratorFunction<Long, Tuple2<String, String>> {

    // 定义随机数数据生成器
    public RandomDataGenerator generator;

    // 初始化随机数数据生成器
    @Override
    public void open(SourceReaderContext readerContext) {
        generator = new RandomDataGenerator();
    }

    @Override
    public Tuple2<String, String> map(Long key) {
        String value = "{\"xid\":\"2443432332\",\"binlog\":\"1111111111@dd-bin.000254\",\"time\":1710917217000,\"canalTime\":1710917217506,\"db\":\"a_b_c\",\"table\":\"xxx_tb\",\"event\":\"d\",\"columns\":[{\"n\":\"id\",\"t\":\"bigint(20) unsigned\",\"v\":\"344107022\",\"null\":false},{\"n\":\"created_by\",\"t\":\"bigint(20) unsigned\",\"v\":\"0\",\"null\":false},{\"n\":\"creation_date\",\"t\":\"bigint(20) unsigned\",\"v\":\"1702775888134\",\"null\":false}],\"keys\":[\"id\"]}";
        String string = generator.nextHexString(4);// 生成随机的4位字符串
        return Tuple2.of(key + " " + string, value);
    }
}