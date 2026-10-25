package com.blanka.cdc.autoconfigure.client;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.blanka.cdc.autoconfigure.BlankaCdcProperties;
import com.blanka.cdc.autoconfigure.dto.BlankaBinlogDTO;
import com.blanka.cdc.autoconfigure.enums.BatchStrategyEnum;
import com.blanka.cdc.autoconfigure.route.RouteResult;
import com.blanka.cdc.autoconfigure.route.TableRouteMatcher;
import jakarta.annotation.PreDestroy;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * blanka cdc 核心客户端
 * <p>
 * 项目启动后新开独立消费线程消费canal：
 * 连接canal-server → 循环getWithoutAck拉取batch → 黑名单/DDL过滤 → 路由决策
 * → 组装BlankaBinlogDTO → 发送RocketMQ → 按BatchStrategyEnum执行ack/rollback
 *
 * @author blanka
 */
public class BlankaCdcClient implements CommandLineRunner {

    /** canal消费线程固定命名 */
    public static final String CONSUME_THREAD_NAME = "blanka-cdc-consume";

    private static final Logger log = LoggerFactory.getLogger(BlankaCdcClient.class);

    /** 单次拉取batch大小 */
    private static final int DEFAULT_BATCH_SIZE = 1000;
    /** 订阅所有库表 */
    private static final String SUBSCRIBE_FILTER = ".*\\..*";
    /** 空batch休眠时间 */
    private static final long EMPTY_BATCH_SLEEP_MS = 1000L;
    /** 断连重连间隔 */
    private static final long RECONNECT_INTERVAL_MS = 5000L;
    /** rollback后重试间隔，防止故障时紧密循环 */
    private static final long ROLLBACK_RETRY_INTERVAL_MS = 1000L;

    private final BlankaCdcProperties properties;
    private final TableRouteMatcher routeMatcher;
    private final DefaultMQProducer producer;
    private final List<Pattern> excludePatterns = new ArrayList<>();

    private volatile boolean running = true;
    private volatile Thread consumeThread;
    private volatile CanalConnector connector;

    public BlankaCdcClient(BlankaCdcProperties properties, TableRouteMatcher routeMatcher, DefaultMQProducer producer) {
        this.properties = properties;
        this.routeMatcher = routeMatcher;
        this.producer = producer;
        compileExcludePatterns();
    }

    /**
     * 启动独立canal消费线程
     */
    @Override
    public void run(String... args) {
        Thread thread = new Thread(this::consumeLoop, CONSUME_THREAD_NAME);
        thread.setDaemon(true);
        this.consumeThread = thread;
        thread.start();
        log.info("[blanka-cdc] client started, thread={}, canalServerAddr={}, destination={}, batchStrategy={}",
                CONSUME_THREAD_NAME, properties.getCanalServerAddr(), properties.getDestination(),
                properties.getBatchStrategy());
    }

    /**
     * 停止消费线程并断开canal连接；producer生命周期由Spring容器管理
     */
    @PreDestroy
    public void shutdown() {
        running = false;
        Thread thread = this.consumeThread;
        if (thread != null) {
            thread.interrupt();
        }
        quietDisconnect();
        log.info("[blanka-cdc] client shutdown");
    }

    // ---------------------------------------------------------------- consume

    private void consumeLoop() {
        while (running) {
            try {
                connector = createConnector();
                connector.connect();
                connector.subscribe(SUBSCRIBE_FILTER);
                connector.rollback();
                log.info("[blanka-cdc] connected to canal-server, addr={}, destination={}",
                        properties.getCanalServerAddr(), properties.getDestination());

                while (running) {
                    Message message = connector.getWithoutAck(DEFAULT_BATCH_SIZE);
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        quietSleep(EMPTY_BATCH_SLEEP_MS);
                        continue;
                    }
                    handleBatch(batchId, message.getEntries());
                }
            } catch (IllegalArgumentException e) {
                // 配置类错误：重试无意义，终止消费线程
                log.error("[blanka-cdc] invalid canal config, consume thread exit", e);
                return;
            } catch (Exception e) {
                if (!running) {
                    // 已收到停止信号（destroy中断线程），不再重连
                    return;
                }
                log.error("[blanka-cdc] consume error, will reconnect after {}ms, canalServerAddr={}",
                        RECONNECT_INTERVAL_MS, properties.getCanalServerAddr(), e);
                quietDisconnect();
                quietSleep(RECONNECT_INTERVAL_MS);
            }
        }
    }

    /**
     * 按batch策略处理：ROLLBACK_ALL整批一致 / LOCAL_TABLE立刻ack
     */
    private void handleBatch(long batchId, List<CanalEntry.Entry> entries) {
        if (properties.getBatchStrategy() == BatchStrategyEnum.LOCAL_TABLE) {
            // LOCAL_TABLE（V1.0骨架）：收到batch立刻ack；发送失败只打错误日志
            // 约束：消费端必须实现幂等，会重复投递；V1.2补齐本地投递表+重试死信
            try {
                connector.ack(batchId);
            } catch (Exception e) {
                log.error("[blanka-cdc] LOCAL_TABLE ack error, batchId={}", batchId, e);
                return;
            }
            try {
                processEntries(batchId, entries);
            } catch (Exception e) {
                log.error("[blanka-cdc] LOCAL_TABLE send fail (batch already acked, consumer MUST be idempotent), batchId={}",
                        batchId, e);
            }
            return;
        }

        // ROLLBACK_ALL（默认）：任意一条发送RocketMQ异常，整batch rollback，不ack，下一次循环重新消费整个batch
        try {
            processEntries(batchId, entries);
            connector.ack(batchId);
        } catch (Exception e) {
            log.error("[blanka-cdc] batch send fail, rollback whole batch, batchId={}", batchId, e);
            try {
                connector.rollback(batchId);
            } catch (Exception rollbackError) {
                log.error("[blanka-cdc] rollback error, batchId={}", batchId, rollbackError);
            }
            quietSleep(ROLLBACK_RETRY_INTERVAL_MS);
        }
    }

    /**
     * 遍历Entry：黑名单过滤、DDL过滤；遍历RowData路由、组装DTO、发送MQ
     */
    private void processEntries(long batchId, List<CanalEntry.Entry> entries) throws Exception {
        for (CanalEntry.Entry entry : entries) {
            // 非ROWDATA：TRANSACTIONBEGIN / TRANSACTIONEND / HEARTBEAT等
            // skip-ddl=true时全部跳过（DDL、TRANSACTIONBEGIN、TRANSACTIONEND）
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                if (properties.isSkipDdl()) {
                    log.debug("[blanka-cdc] skip non-ROWDATA entry, type={}, batchId={}", entry.getEntryType(), batchId);
                }
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                // 单条解析失败跳过，避免毒丸entry阻塞整个batch
                log.warn("[blanka-cdc] parse RowChange fail, skip entry, batchId={}", batchId, e);
                continue;
            }

            // DDL过滤
            if (rowChange.getIsDdl()) {
                if (properties.isSkipDdl()) {
                    log.debug("[blanka-cdc] skip DDL, schema={}, table={}, sql={}, batchId={}",
                            entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                            rowChange.getSql(), batchId);
                    continue;
                }
                // skip-ddl=false：V1.0统一DTO为行级协议，DDL无行数据，走行循环自然不产生消息
            }

            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            // 黑名单：fullTable匹配exclude-patterns正则，命中直接丢弃不发MQ
            if (isExcluded(schemaName, tableName)) {
                log.debug("[blanka-cdc] exclude table hit, skip, fullTable={}, batchId={}",
                        schemaName + "." + tableName, batchId);
                continue;
            }

            CanalEntry.EventType eventType = rowChange.getEventType();
            long executeTime = entry.getHeader().getExecuteTime();

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                RouteResult route = routeMatcher.match(schemaName, tableName, rowChange);
                BlankaBinlogDTO dto = buildBinlogDTO(batchId, schemaName, tableName,
                        eventType, executeTime, rowData, route);
                sendToMq(dto, route);
            }
        }
    }

    // ---------------------------------------------------------------- dto build

    private BlankaBinlogDTO buildBinlogDTO(long batchId, String schemaName, String tableName,
                                           CanalEntry.EventType eventType, long executeTime,
                                           CanalEntry.RowData rowData, RouteResult route) {
        BlankaBinlogDTO dto = new BlankaBinlogDTO();
        dto.setSchemaName(schemaName);
        dto.setTableName(tableName);
        dto.setEventType(eventType.name());
        dto.setPk(extractPk(rowData));
        dto.setBeforeData(columnsToMap(rowData.getBeforeColumnsList()));
        dto.setAfterData(columnsToMap(rowData.getAfterColumnsList()));
        dto.setRouteTopic(route.getTopic());
        dto.setRouteTag(route.getTag());
        dto.setRouteType(route.getRouteType());
        dto.setTs(executeTime);
        dto.setBatchId(batchId);
        return dto;
    }

    /**
     * pk：优先取主键列的值；多主键取第一个主键；无主键为空字符串
     */
    private String extractPk(CanalEntry.RowData rowData) {
        // 优先after列（INSERT/UPDATE），DELETE时after为空再取before列
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            if (column.getIsKey()) {
                return column.getValue();
            }
        }
        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
            if (column.getIsKey()) {
                return column.getValue();
            }
        }
        return "";
    }

    /**
     * 列集合转Map：columnName -> value；INSERT时before为空Map，DELETE时after为空Map
     */
    private Map<String, Object> columnsToMap(List<CanalEntry.Column> columns) {
        Map<String, Object> map = new LinkedHashMap<>(Math.max(8, columns.size()));
        for (CanalEntry.Column column : columns) {
            map.put(column.getName(), column.getValue());
        }
        return map;
    }

    // ---------------------------------------------------------------- send

    private void sendToMq(BlankaBinlogDTO dto, RouteResult route) throws Exception {
        try {
            org.apache.rocketmq.common.message.Message mqMessage = new org.apache.rocketmq.common.message.Message(
                    route.getTopic(), route.getTag(), buildMessageKey(dto),
                    JSON.toJSONString(dto).getBytes(StandardCharsets.UTF_8));
            SendResult sendResult = producer.send(mqMessage);
            if (sendResult != null && sendResult.getSendStatus() != SendStatus.SEND_OK) {
                log.warn("[blanka-cdc] send status not SEND_OK, status={}, batchId={}, schemaName={}, tableName={}, targetTopic={}",
                        sendResult.getSendStatus(), dto.getBatchId(), dto.getSchemaName(), dto.getTableName(),
                        route.getTopic());
            }
            log.debug("[blanka-cdc] send success, batchId={}, schemaName={}, tableName={}, targetTopic={}, routeType={}",
                    dto.getBatchId(), dto.getSchemaName(), dto.getTableName(), route.getTopic(), route.getRouteType());
        } catch (Exception e) {
            // 发送失败日志必须打印：batchId、schemaName、tableName、targetTopic、异常栈
            log.error("[blanka-cdc] send to rocketmq fail, batchId={}, schemaName={}, tableName={}, targetTopic={}",
                    dto.getBatchId(), dto.getSchemaName(), dto.getTableName(), route.getTopic(), e);
            throw e;
        }
    }

    /**
     * 消息key：schema.table.pk，便于消费端幂等与排查
     */
    private String buildMessageKey(BlankaBinlogDTO dto) {
        return dto.getSchemaName() + "." + dto.getTableName() + "." + dto.getPk();
    }

    // ---------------------------------------------------------------- filter

    /**
     * 启动时编译黑名单正则
     */
    private void compileExcludePatterns() {
        if (properties.getExcludePatterns() != null) {
            for (String pattern : properties.getExcludePatterns()) {
                if (StrUtil.isNotBlank(pattern)) {
                    excludePatterns.add(Pattern.compile(pattern));
                }
            }
        }
    }

    private boolean isExcluded(String schemaName, String tableName) {
        if (excludePatterns.isEmpty()) {
            return false;
        }
        String fullTable = schemaName + "." + tableName;
        for (Pattern pattern : excludePatterns) {
            if (pattern.matcher(fullTable).matches()) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------- connector

    private CanalConnector createConnector() {
        String addr = properties.getCanalServerAddr();
        List<String> parts = StrUtil.split(addr, ':', true, true);
        if (parts.size() != 2 || StrUtil.isBlank(parts.get(0)) || StrUtil.isBlank(parts.get(1))) {
            throw new IllegalArgumentException("[blanka-cdc] canal-server-addr must be host:port, but got: " + addr);
        }
        return CanalConnectors.newSingleConnector(
                new InetSocketAddress(parts.get(0), Integer.parseInt(parts.get(1))),
                properties.getDestination(), properties.getUsername(), properties.getPassword());
    }

    private void quietDisconnect() {
        CanalConnector current = this.connector;
        if (current != null) {
            try {
                current.disconnect();
            } catch (Exception e) {
                log.debug("[blanka-cdc] canal disconnect error", e);
            }
        }
    }

    private void quietSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
