package com.blanka.cdc.autoconfigure.route;

import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.blanka.cdc.autoconfigure.BlankaCdcProperties;
import com.blanka.cdc.autoconfigure.spi.BlankaRouteStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 表路由决策器
 * <p>
 * 严格按路由优先级返回RouteResult（顺序不可调换）：
 * <ol>
 *     <li>SPI BlankaRouteStrategy：任意一个返回非null，直接使用，routeType=SPI</li>
 *     <li>yaml route-rules：正则匹配 schema.table，命中使用配置topic/tag，routeType=CONFIG</li>
 *     <li>自动路由：autoTopicPrefix + schema + "_" + table，非法字符替换为"_"，超长降级default-topic，routeType=AUTO</li>
 *     <li>兜底default-topic，routeType=DEFAULT</li>
 * </ol>
 *
 * @author blanka
 */
public class TableRouteMatcher {

    public static final String ROUTE_TYPE_SPI = "SPI";
    public static final String ROUTE_TYPE_CONFIG = "CONFIG";
    public static final String ROUTE_TYPE_AUTO = "AUTO";
    public static final String ROUTE_TYPE_DEFAULT = "DEFAULT";

    private static final Logger log = LoggerFactory.getLogger(TableRouteMatcher.class);

    /** 自动topic非法字符：非字母数字下划线 */
    private static final String ILLEGAL_TOPIC_CHAR_REGEX = "[^a-zA-Z0-9_]";
    /** rocketmq topic最大长度 */
    private static final int MAX_TOPIC_LENGTH = 64;

    private final BlankaCdcProperties properties;
    private final List<BlankaRouteStrategy> routeStrategies;
    private volatile List<CompiledRule> compiledRules = Collections.emptyList();

    public TableRouteMatcher(BlankaCdcProperties properties, List<BlankaRouteStrategy> routeStrategies) {
        this.properties = properties;
        this.routeStrategies = routeStrategies == null ? Collections.emptyList() : routeStrategies;
        compileRules();
    }

    /**
     * 启动时编译route-rules正则Pattern；预留给V1.1动态刷新使用（重新编译正则）
     */
    public synchronized void compileRules() {
        List<CompiledRule> rules = new ArrayList<>();
        if (properties.getRouteRules() != null) {
            for (BlankaCdcProperties.TableRouteRule rule : properties.getRouteRules()) {
                if (rule == null || !rule.isEnable() || StrUtil.isBlank(rule.getTablePattern())) {
                    continue;
                }
                rules.add(new CompiledRule(Pattern.compile(rule.getTablePattern()), rule.getTopic(), rule.getTag()));
            }
        }
        this.compiledRules = rules;
        log.info("[blanka-cdc] route rules compiled, size={}", rules.size());
    }

    /**
     * 路由决策：SPI > yaml route-rules > 自动路由 > 默认兜底
     */
    public RouteResult match(String schemaName, String tableName, CanalEntry.RowChange rowChange) {
        final String fullTable = schemaName + "." + tableName;

        // 1. SPI BlankaRouteStrategy：任意一个返回非null RouteResult，直接使用
        for (BlankaRouteStrategy strategy : routeStrategies) {
            RouteResult result = strategy.route(schemaName, tableName, rowChange);
            if (result != null) {
                if (StrUtil.isBlank(result.getRouteType())) {
                    result.setRouteType(ROUTE_TYPE_SPI);
                }
                return result;
            }
        }

        // 2. yaml route-rules：正则匹配 schema.table
        for (CompiledRule rule : compiledRules) {
            if (rule.pattern.matcher(fullTable).matches()) {
                return new RouteResult(rule.topic, rule.tag, ROUTE_TYPE_CONFIG);
            }
        }

        // 3. 自动路由（auto-route-enable=true）
        if (properties.isAutoRouteEnable()) {
            String topic = properties.getAutoTopicPrefix() + schemaName + "_" + tableName;
            topic = topic.replaceAll(ILLEGAL_TOPIC_CHAR_REGEX, "_");
            if (topic.length() > MAX_TOPIC_LENGTH) {
                log.warn("[blanka-cdc] auto topic too long(length={}), degrade to default-topic, fullTable={}",
                        topic.length(), fullTable);
                topic = properties.getDefaultTopic();
            }
            // tag = schema.table
            return new RouteResult(topic, fullTable, ROUTE_TYPE_AUTO);
        }

        // 4. 兜底default-topic
        return new RouteResult(properties.getDefaultTopic(), fullTable, ROUTE_TYPE_DEFAULT);
    }

    /**
     * 编译后的yaml路由规则
     */
    private static class CompiledRule {
        private final Pattern pattern;
        private final String topic;
        private final String tag;

        CompiledRule(Pattern pattern, String topic, String tag) {
            this.pattern = pattern;
            this.topic = topic;
            this.tag = tag;
        }
    }
}
