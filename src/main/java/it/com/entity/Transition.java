package it.com.entity;

import it.com.enums.State;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 状态转移类，包含当前引起状态转移的事件类型，以及转移的目标状态
 */
@AllArgsConstructor
@Getter
public class Transition {
    /**
     * 触发状态转移的当前事件类型
     */
    private final String eventType;

    /**
     * 转移的目标状态
     */
    private final State targetState;
}