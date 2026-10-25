package com.blanka.cdc.autoconfigure.dto;

import lombok.Data;

import java.util.Map;

/**
 * 统一对外Binlog消息DTO：所有路由分支输出同一份结构，消费端不需要区分路由来源
 *
 * @author blanka
 */
@Data
public class BlankaBinlogDTO {
    private String schemaName;
    private String tableName;
    private String eventType;
    private String pk;
    private Map<String, Object> beforeData;
    private Map<String, Object> afterData;
    private String routeTopic;
    private String routeTag;
    private String routeType;
    private long ts; // mysql执行时间戳
    private long batchId; // canal batchId
}
