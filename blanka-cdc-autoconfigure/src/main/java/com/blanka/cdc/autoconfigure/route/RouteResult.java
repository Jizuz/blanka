package com.blanka.cdc.autoconfigure.route;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 路由结果：目标topic、tag、路由来源类型
 *
 * @author blanka
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RouteResult {
    private String topic;
    private String tag;
    private String routeType; // AUTO / CONFIG / SPI / DEFAULT
}
