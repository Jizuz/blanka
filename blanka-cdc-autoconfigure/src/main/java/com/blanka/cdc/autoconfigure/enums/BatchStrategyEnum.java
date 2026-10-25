package com.blanka.cdc.autoconfigure.enums;

/**
 * batch 处理策略
 *
 * @author blanka
 */
public enum BatchStrategyEnum {
    /**
     * 整批一致性优先：任意一条发送失败，整个batch rollback，位点不提交
     */
    ROLLBACK_ALL,
    /**
     * 吞吐优先：收到batch立刻ack给canal；V1.0仅骨架，V1.2实现本地投递表+重试死信
     */
    LOCAL_TABLE
}
