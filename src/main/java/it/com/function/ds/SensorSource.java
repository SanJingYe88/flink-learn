package it.com.function.ds;

import it.com.entity.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * 传感器数据源
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> srcCtx) throws Exception {
        Random rand = new Random();

        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + i;
            curFTemp[i] = 60 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long curTime = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                srcCtx.collect(new SensorReading(sensorIds[i], curFTemp[i], curTime));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}