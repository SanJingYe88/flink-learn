package it.com.entity;

import lombok.Data;

/**
 * 执行结果
 */
@Data
public class WorkResult {
    private String workId;
    private String instanceId;
    private int passFlag;
    private long ts;
}
