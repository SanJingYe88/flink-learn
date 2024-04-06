package it.com.entity;

import lombok.Data;

@Data
public class LoginEvent {
    private String userId;
    private String ipAddress;
    private String eventType;
    private Long timestamp;
}