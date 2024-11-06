package it.com.function.generator;

import it.com.entity.WorkResult;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

/**
 * mock数据生成 function
 */
public class WorkResultGeneratorFunction implements GeneratorFunction<Long, WorkResult> {
    public RandomDataGenerator generator;

    @Override
    public void open(SourceReaderContext readerContext) {
        generator = new RandomDataGenerator();
    }

    @Override
    public WorkResult map(Long key) {
        WorkResult workResult = new WorkResult();
        workResult.setWorkId(System.currentTimeMillis() % 2 == 0 ? "work-a" : "work-b");
        workResult.setInstanceId(System.currentTimeMillis() % 2 == 0 ? "1" : "2");
        workResult.setPassFlag(System.currentTimeMillis() % 2 == 0 ? 0 : 1);
        workResult.setTs(System.currentTimeMillis());
        return workResult;
    }
}