package it.com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class LoginEvent {
    private String userId; //用户ID
    private String ipAddress; //IP地址
    private String eventType; //状态
    private Long timestamp; //时间戳
}
