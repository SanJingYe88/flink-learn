package it.com.function.window;

import it.com.entity.WorkPassRate;
import it.com.entity.WorkResult;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 通过率计算聚合函数
 */
public class PassRateAggregateFunction implements AggregateFunction<WorkResult, WorkPassRate, WorkPassRate> {

    /**
     * 初始化累加器
     * @return
     */
    @Override
    public WorkPassRate createAccumulator() {
        WorkPassRate workPassRate = new WorkPassRate();
        workPassRate.setPassRate(0.0);
        workPassRate.setWorkId(null);
        workPassRate.setPassNum(0);
        workPassRate.setTotalNum(0);
        return workPassRate;
    }

    @Override
    public WorkPassRate add(WorkResult value, WorkPassRate acc) {
        acc.setWorkId(value.getWorkId());
        acc.setCalTime(System.currentTimeMillis());
        acc.setPassNum(acc.getPassNum() + (value.getPassFlag() == 1 ? 1 : 0));
        acc.setTotalNum(acc.getTotalNum() + 1);
        return acc;
    }

    @Override
    public WorkPassRate getResult(WorkPassRate acc) {
        return acc;
    }

    @Override
    public WorkPassRate merge(WorkPassRate a, WorkPassRate b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }

        WorkPassRate workPassRate = new WorkPassRate();
        workPassRate.setWorkId(a.getWorkId());
        workPassRate.setCalTime(System.currentTimeMillis());
        workPassRate.setPassNum(a.getPassNum() + b.getPassNum());
        workPassRate.setTotalNum(a.getTotalNum() + b.getTotalNum());
        workPassRate.setPassRate(calculatePassRate(workPassRate));
        return workPassRate;
    }

    /**
     * 计算通过率
     */
    private double calculatePassRate(WorkPassRate result) {
        if (result.getTotalNum() == 0) {
            return 0.0;
        }
        return (double) result.getPassNum() / result.getTotalNum();
    }
}