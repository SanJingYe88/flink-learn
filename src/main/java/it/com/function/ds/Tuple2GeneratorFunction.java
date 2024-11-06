package it.com.function.ds;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class Tuple2GeneratorFunction implements GeneratorFunction<Long, Tuple2<String, String>> {

    /**
     * 定义随机数数据生成器
     */
    public RandomDataGenerator generator;

    /**
     * 初始化随机数数据生成器
     * @param readerContext
     * @throws Exception
     */
    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        generator = new RandomDataGenerator();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Tuple2<String, String> map(Long key) throws Exception {
        String value = "xxxxxxx";
        return Tuple2.of("" + key, value);
    }
}