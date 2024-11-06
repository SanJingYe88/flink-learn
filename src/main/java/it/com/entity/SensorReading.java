package it.com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 传感器读数实体类
 * 表示传感器在某一时刻的温度读数
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class SensorReading {
    /**
     * 传感器ID
     */
    private String id;

    /**
     * 温度读数
     */
    private double temperature;

    /**
     * 读数时间戳
     */
    private long timestamp;
}
