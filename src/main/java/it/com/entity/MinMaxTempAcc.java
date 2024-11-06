package it.com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 温度最大最小值累加器
 * 用于存储聚合过程中的中间结果
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class MinMaxTempAcc {
    private double min;
    private double max;
}