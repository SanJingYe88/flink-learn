package it.com.entity;

import lombok.Data;

/**
 * 汽车超速信息
 */
@Data
public class CarOverSpeedLimitInfo {
    private String id;
    // 城市
    private int cityId;
    // 街道
    private int streetId;
    // 路口
    private int crossroadId;
    // 车牌号
    private String carNo;
    // 限速
    private double limitSpeed;
    // 当前时速
    private double currentSpeed;
    // 时间戳
    private long ts;
}
