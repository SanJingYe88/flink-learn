package it.com.entity;

import lombok.Data;

/**
 * 执行通过率
 */
@Data
public class WorkPassRate {
    private String workId;
    private int passNum;
    private int totalNum;
    private double passRate;
    private long calTime;
    private long windowStart;
    private long windowEnd;
}
