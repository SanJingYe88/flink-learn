package it.com.entity;

import lombok.Data;

/**
 * 涉嫌套牌车辆信息
 */
@Data
public class RepatitionCarInfo {
    // 城市
    private int cityId;
    // 街道
    private int streetId;
    // 路口
    private int crossroadId;
    // 车牌号
    private String carNo;
    // 本次时间
    private long thisTime;
    // 上次时间
    private long lastTime;
}
