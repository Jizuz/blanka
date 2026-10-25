package com.blanka.cdc.autoconfigure;

import com.blanka.cdc.autoconfigure.enums.BatchStrategyEnum;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * blanka cdc 配置绑定类，前缀 blanka.cdc，所有属性提供默认值
 *
 * @author blanka
 */
@Data
@ConfigurationProperties(prefix = "blanka.cdc")
public class BlankaCdcProperties {

    /** 是否启用，默认true；不写配置段也生效 */
    private boolean enable = true;

    /** canal tcp连接地址 host:port；为空时按优先级补全（SPI > rocketmq全局 > 环境变量CANAL_SERVER_ADDR） */
    private String canalServerAddr = "";

    /** canal destination */
    private String destination = "example";

    /** canal 用户名 */
    private String username = "";

    /** canal 密码 */
    private String password = "";

    /** rocketmq nameserver地址；为空时按优先级补全（SPI > rocketmq.name-server > 环境变量ROCKETMQ_NAMESRV_ADDR） */
    private String namesrvAddr = "";

    /** rocketmq producer group；为空时按优先级补全（SPI > rocketmq.producer.group） */
    private String producerGroup = "";

    /** 自动路由开关，默认true */
    private boolean autoRouteEnable = true;

    /** 自动路由topic前缀 */
    private String autoTopicPrefix = "blanka_binlog_";

    /** 兜底topic */
    private String defaultTopic = "blanka_binlog_default";

    /** DDL控制：true时跳过DDL、TRANSACTIONBEGIN、TRANSACTIONEND */
    private boolean skipDdl = true;

    /** batch策略：ROLLBACK_ALL / LOCAL_TABLE */
    private BatchStrategyEnum batchStrategy = BatchStrategyEnum.ROLLBACK_ALL;

    /** 黑名单，正则匹配 fullTable（schema.table），命中直接丢弃不发MQ */
    private List<String> excludePatterns = new ArrayList<>();

    /** 自定义路由规则，可选 */
    private List<TableRouteRule> routeRules = new ArrayList<>();

    /**
     * yaml静态路由规则
     */
    @Data
    public static class TableRouteRule {
        /** 规则是否启用 */
        private boolean enable = true;
        /** 表匹配正则，匹配 schema.table */
        private String tablePattern;
        /** 目标topic */
        private String topic;
        /** 消息tag */
        private String tag;
    }
}
