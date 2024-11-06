package it.com.enums;

import it.com.entity.Transition;

/**
 * 状态枚举
 */
public enum State {

    // 匹配失败，终止状态
    Terminal,
    //匹配成功
    Matched,
    // S2状态, 遇到 fail 事件，转到 Matched，遇到 success 事件，转到 Terminal
    S2(new Transition("fail", Matched), new Transition("success", Terminal)),
    // S1状态，遇到 fail 事件，转到 S2，遇到 success 事件，转到 Terminal
    S1(new Transition("fail", S2), new Transition("success", Terminal)),
    // 初始状态,遇到 fail 事件，转到 S1，遇到 success 事件，转到 Terminal
    Initial(new Transition("fail", S1), new Transition("success", Terminal));

    /**
     * 当前状态的转移机制
     */
    private final Transition[] transitions;

    /**
     * 状态的构造方法，可以传入一组状态转移规则来定义状态
     *
     * @param transitions transitions
     */
    State(Transition... transitions) {
        this.transitions = transitions;
    }

    /**
     * 状态的转移方法，根据当前输入事件类型，从定义好的转移规则中找到下一个状态
     *
     * @param eventType eventType
     * @return
     */
    public State transition(String eventType) {
        for (Transition transition : transitions) {
            if (transition.getEventType().equals(eventType)) {
                return transition.getTargetState();
            }
        }
        // 如果没有找到转移规则，说明已经结束，回到初始状态
        return Initial;
    }
}
