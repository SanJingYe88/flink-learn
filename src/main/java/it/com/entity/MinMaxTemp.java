package it.com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器温度统计实体类
 * 用于存储每个传感器在窗口期内的最高温度和最低温度
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class MinMaxTemp {
    /**
     * 传感器ID
     */
    private String id;

    /**
     * 窗口内最低温度
     */
    private Double min;

    /**
     * 窗口内最高温度
     */
    private Double max;

    /**
     * 窗口结束时间戳
     */
    private Long endTs;
}