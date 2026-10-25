package com.blanka.cdc.autoconfigure.spi;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.blanka.cdc.autoconfigure.route.RouteResult;

/**
 * 自定义路由SPI，优先级高于yaml路由规则与自动路由
 * <p>
 * SPI加载方式：Java ServiceLoader，META-INF/services 注册实现类；starter自动注册到Spring上下文，list注入。
 *
 * @author blanka
 */
public interface BlankaRouteStrategy {

    /**
     * 路由决策
     *
     * @param schemaName 库名
     * @param tableName  表名
     * @param rowChange  canal行变更
     * @return null：放弃本层路由，进入下一级路由逻辑；非null则直接使用该路由结果（routeType=SPI）
     */
    RouteResult route(String schemaName, String tableName, CanalEntry.RowChange rowChange);
}
