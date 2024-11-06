package it.com.function.ds;

import it.com.enums.SmokeLevel;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 烟雾等级 数据源
 */
public class SmokeLevelSource implements SourceFunction<SmokeLevel> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> srcCtx) throws Exception {
        Random rand = new Random();
        while (running) {
            if (rand.nextGaussian() > 0.8) {
                srcCtx.collect(SmokeLevel.HIGH);
            } else {
                srcCtx.collect(SmokeLevel.LOW);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}