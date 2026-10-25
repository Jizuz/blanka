package com.blanka.cdc.demo.consumer;

import com.blanka.cdc.autoconfigure.dto.BlankaBinlogDTO;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * demo消费者示例：
 * 自动路由topic规则 = autoTopicPrefix + schema + "_" + table，即 blanka_binlog_db_order_order_info
 *
 * @author blanka
 */
@Component
@RocketMQMessageListener(topic = "blanka_binlog_db_order_order_info", consumerGroup = "order_consumer")
public class OrderBinlogConsumer implements RocketMQListener<BlankaBinlogDTO> {

    private static final Logger log = LoggerFactory.getLogger(OrderBinlogConsumer.class);

    @Override
    public void onMessage(BlankaBinlogDTO dto) {
        // 业务消费，必须幂等
        log.info("[demo] receive binlog: schema={}, table={}, eventType={}, pk={}, before={}, after={}, "
                        + "routeTopic={}, routeTag={}, routeType={}, ts={}, batchId={}",
                dto.getSchemaName(), dto.getTableName(), dto.getEventType(), dto.getPk(),
                dto.getBeforeData(), dto.getAfterData(),
                dto.getRouteTopic(), dto.getRouteTag(), dto.getRouteType(), dto.getTs(), dto.getBatchId());
    }
}
