package it.com.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 浏览时长
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class ViewTimeLenEvent {
    private int id;
    private String name;
    private String url;
    private int viewTimeLen;
    private long ts;
}
