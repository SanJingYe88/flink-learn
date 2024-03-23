package it.com.entity;

import lombok.Data;

/**
 * 街道限速规则
 */
@Data
public class StreetSpeedLimitRule {
    private String id;
    // 城市
    private int cityId;
    // 街道
    private int streetId;
    // 路口
    private int crossroadId;
    // 限速规则
    private double limitSpeed;
    // 更新时间
    private long updateTime;
}
