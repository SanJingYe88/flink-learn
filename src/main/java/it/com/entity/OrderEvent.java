package it.com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class OrderEvent {
    private String userId; //用户ID
    private String orderId; //订单ID
    private String eventType; //事件类型（操作类型）
    private Long timestamp; //时间戳
}
