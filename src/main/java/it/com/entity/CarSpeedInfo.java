package it.com.entity;

import lombok.Data;

/**
 * 汽车路过速度信息
 */
@Data
public class CarSpeedInfo {
    private String id;
    // 城市
    private int cityId;
    // 街道
    private int streetId;
    // 路口
    private int crossroadId;
    // 车牌号
    private String carNo;
    // 当前时速
    private double currentSpeed;
    // 时间戳
    private long ts;
}
