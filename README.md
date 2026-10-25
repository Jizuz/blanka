# blanka-cdc

**MySQL Binlog → RocketMQ 轻量级 CDC Starter（Spring Boot 3）**

业务应用只需引入一个 starter、写一份 yaml，即可把 MySQL 的行级变更（INSERT / UPDATE / DELETE）实时投递到 RocketMQ，消费端用统一的 `BlankaBinlogDTO` 结构接收，无需感知 canal 细节。

## 特性一览

- **零侵入**：不解析业务代码，基于 canal-server 订阅 binlog，行级变更实时投递
- **极简接入**：不写任何 `blanka.cdc` 配置也能跑 —— 自动拾取项目已有的 `rocketmq.*` 全局配置，canal 地址用环境变量注入即可
- **四级路由**：SPI 自定义路由 > yaml 静态规则 > 自动路由 > 兜底 topic，一表一 topic 或统一 topic 皆可
- **统一消息模型**：所有路由分支输出同一份 `BlankaBinlogDTO`（含 before/after 镜像、pk、事件类型），消费端零分支逻辑
- **可靠性语义可选**：`ROLLBACK_ALL` 整批一致（默认）/ `LOCAL_TABLE` 吞吐优先
- **过滤能力**：库表白名单语义的黑名单正则（exclude-patterns）、DDL / 事务事件过滤（skip-ddl）
- **SPI 扩展**：基础设施地址（`InfraConfigProvider`）与路由策略（`BlankaRouteStrategy`）均可用 Java ServiceLoader 扩展，自动注册为 Spring Bean
- **快速失败**：关键配置缺失时启动即报错（Assert），不带入运行期才暴露的隐患

---

## 一、整体架构

### 1.1 端到端数据流

```
┌──────────┐  binlog(ROW) ┌──────────────┐  TCP getWithoutAck  ┌─────────────────────────────────────┐
│  MySQL   │ ───────────> │ canal-server │ ──────────────────> │        业务应用（引入本 starter）      │
│          │              │  （独立部署）  │                     │                                     │
└──────────┘              └──────────────┘                     │  BlankaCdcClient                     │
                                                               │  ┌─────────────────────────────┐     │
                                                               │  │ thread: blanka-cdc-consume  │     │
                                                               │  │  黑名单过滤 → DDL过滤        │     │
                                                               │  │  → 路由决策 → 组装DTO        │     │
                                                               │  │  → 发送RocketMQ → ack/rollback│    │
                                                               │  └─────────────┬───────────────┘     │
                                                               └────────────────┼────────────────────┘
                                                                                │ send(topic, tag, key)
                                                                        ┌───────▼───────┐
                                                                        │    RocketMQ   │
                                                                        └───────┬───────┘
                                                                                ▼
                                                          业务消费者 @RocketMQMessageListener
                                                          （RocketMQListener<BlankaBinlogDTO>）
```

### 1.2 模块划分

| 模块 | 职责 | 依赖方 |
|---|---|---|
| `blanka-cdc-autoconfigure` | 全部核心实现：自动配置、配置补全、SPI 注册器、路由决策、消费客户端、DTO | — |
| `blanka-cdc-spring-boot-starter` | 空壳聚合模块，仅依赖 autoconfigure（标准 starter 结构） | 业务应用 |
| `blanka-cdc-demo` | 演示工程：极简接入 + 消费者示例 | starter |

```xml
<dependency>
    <groupId>com.blanka.cdc</groupId>
    <artifactId>blanka-cdc-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

技术栈：Spring Boot 3.2.x / Java 17 / canal-client 1.1.7 / rocketmq-client 5.3.0 / rocketmq-spring-boot 2.3.1

---

## 二、核心设计

### 2.1 自动装配与配置补全（关键：业务零配置也能跑）

`BlankaCdcAutoConfiguration` 通过 Spring Boot 3 标准的
`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 自动装配，
`blanka.cdc.enable` 缺省即为 `true` —— **引依赖即生效，无需注解**。

基础设施地址（canal 地址、destination、nameserver、producer group）按以下优先级补全，
每一级**只在当前值为空时**生效，已有显式配置不会被覆盖：

```
① SPI InfraConfigProvider（多实现取第一个，非空项覆盖）
        ↓ 为空
② yaml blanka.cdc.* 显式配置
        ↓ 为空
③ 项目全局 RocketMQ 配置自动拾取（rocketmq.name-server / rocketmq.producer.group）
        ↓ 为空
④ 环境变量兜底（CANAL_SERVER_ADDR / CANAL_DESTINATION / ROCKETMQ_NAMESRV_ADDR）
        ↓ 仍为空
⑤ 启动失败（Assert 快速失败，错误信息给出全部可行配置途径）
```

补全完成后打印一条 `infra resolved` 日志，便于确认最终生效地址：

```
[blanka-cdc] infra resolved, canalServerAddr=127.0.0.1:11111, destination=example, namesrvAddr=127.0.0.1:9876, producerGroup=biz_producer_group
```

> 若业务工程已存在 `DefaultMQProducer` Bean，starter 不再重复创建，直接复用（`@ConditionalOnMissingBean`）。

### 2.2 四级路由决策（TableRouteMatcher）

每一条行变更在发送前都会经过路由决策，优先级**从高到低、顺序不可调换**：

| 优先级 | 路由来源 | routeType | 规则 |
|---|---|---|---|
| 1 | SPI `BlankaRouteStrategy` | `SPI` | 任意一个实现返回非 null，直接采用；返回 null 视为放弃本层 |
| 2 | yaml `route-rules` | `CONFIG` | 正则匹配 `schema.table`，命中使用配置的 topic/tag |
| 3 | 自动路由 | `AUTO` | `auto-topic-prefix + schema + "_" + table`，tag 固定为 `schema.table` |
| 4 | 兜底 | `DEFAULT` | `default-topic`（自动路由关闭或 topic 超长时） |

自动路由的健壮性处理：

- 非法字符（非字母数字下划线）统一替换为 `_`，保证 topic 合法
- topic 长度 > 64（RocketMQ 上限）时降级到 `default-topic`，不丢消息

例：`db_order.order_info` 默认自动路由 → topic `blanka_binlog_db_order_order_info`、tag `db_order.order_info`。

### 2.3 统一消息模型（BlankaBinlogDTO）

所有路由分支输出同一份结构，消费端无需区分路由来源：

```jsonc
{
  "schemaName": "db_order",          // 库名
  "tableName":  "order_info",        // 表名
  "eventType":  "UPDATE",            // INSERT / UPDATE / DELETE ...
  "pk":         "10001",             // 主键值（优先after列，DELETE取before；无主键为空串）
  "beforeData": {"id":"10001","status":"PAID"},   // 变更前镜像（INSERT时为空Map）
  "afterData":  {"id":"10001","status":"SHIPPED"},// 变更后镜像（DELETE时为空Map）
  "routeTopic": "blanka_binlog_db_order_order_info",
  "routeTag":   "db_order.order_info",
  "routeType":  "AUTO",              // SPI / CONFIG / AUTO / DEFAULT
  "ts":         1711334400000,       // MySQL 执行时间戳（executeTime）
  "batchId":    1234567890           // canal batchId，同批消息相同，便于对账/排查
}
```

MQ 消息三要素：

- **topic / tag**：来自路由决策结果
- **key**：`schema.table.pk` —— 消费端可基于此做幂等键，也便于按主键查消息轨迹
- **body**：DTO 的 JSON 字符串（fastjson2，UTF-8）

### 2.4 可靠性：batch 处理策略

canal 以 batch 为单位投递（`getWithoutAck(1000)`），starter 提供两种处理语义：

| 策略 | 语义 | 适用场景 |
|---|---|---|
| `ROLLBACK_ALL`（默认） | 逐条发送，**任意一条失败整批 rollback**，位点不提交，下轮重新消费整个 batch | 强一致性优先（默认推荐） |
| `LOCAL_TABLE` | 收到 batch **立刻 ack**，发送失败仅记日志；可能重复投递，消费端必须幂等 | 吞吐优先（V1.0 骨架，V1.2 补齐本地投递表 + 重试死信） |

### 2.5 过滤链

发送前依次执行，命中即丢弃（不占消息量）：

1. **非 ROWDATA 过滤**：`TRANSACTIONBEGIN` / `TRANSACTIONEND` / 心跳等直接跳过
2. **DDL 过滤**（`skip-ddl: true` 时）
3. **黑名单过滤**：`exclude-patterns` 正则匹配 `schema.table`（全匹配），命中直接丢弃

单条 entry 解析失败只跳过该条（warn 日志），不会阻塞整个 batch（防毒丸）。

### 2.6 SPI 扩展机制

starter 内置一个 `BeanDefinitionRegistryPostProcessor`（SPI 注册器），
在 Spring 容器启动早期通过 Java `ServiceLoader` 扫描两个 SPI 接口的实现并**注册为普通 Spring Bean**，
因此实现类里可以直接 `@Resource` 注入业务 Bean、参与 Spring 生命周期：

```
META-INF/services/com.blanka.cdc.autoconfigure.spi.InfraConfigProvider   ← 基础设施地址提供者
META-INF/services/com.blanka.cdc.autoconfigure.spi.BlankaRouteStrategy  ← 自定义路由策略
```

### 2.7 线程模型与生命周期

- 消费线程固定命名 `blanka-cdc-consume`（守护线程），与业务线程池完全隔离，便于监控/排查
- 断连自动重连（5s 间隔），重连期间业务主流程不受影响
- 应用优雅停机时（`@PreDestroy`）：置停标志 → 中断消费线程 → 断开 canal 连接；producer 交由 Spring 容器管理

---

## 三、业务侧使用指南

### 3.1 前置条件

1. MySQL 开启 binlog 且格式为 **ROW**（`binlog_format=ROW`）
2. 已部署 **canal-server**（1.1.7+），并为其配置了 MySQL 主从账号与监听规则
3. 可用的 **RocketMQ** nameserver

### 3.2 三步接入（推荐：极简方式）

**第 1 步：引入依赖**

```xml
<dependency>
    <groupId>com.blanka.cdc</groupId>
    <artifactId>blanka-cdc-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

**第 2 步：配置（业务工程已有 rocketmq 配置时，仅需一个环境变量）**

```yaml
# application.yml —— 业务工程原有的 rocketmq 配置，blanka 自动拾取，无需重复
rocketmq:
  name-server: 127.0.0.1:9876
  producer:
    group: biz_producer_group
```

```bash
# canal 地址用环境变量注入（K8s/容器环境最常见做法）
export CANAL_SERVER_ADDR=127.0.0.1:11111
# 可选：export CANAL_DESTINATION=example   # 默认 example
```

**第 3 步：编写消费者**

```java
@Component
@RocketMQMessageListener(
        topic = "blanka_binlog_db_order_order_info",   // 自动路由规则: 前缀 + 库名 + "_" + 表名
        consumerGroup = "order_consumer")
public class OrderBinlogConsumer implements RocketMQListener<BlankaBinlogDTO> {

    private static final Logger log = LoggerFactory.getLogger(OrderBinlogConsumer.class);

    @Override
    public void onMessage(BlankaBinlogDTO dto) {
        // ⚠️ 消费必须幂等（以 dto 主键 + eventType 为幂等键，或利用 MQ 消息重试语义）
        if ("DELETE".equals(dto.getEventType())) {
            // dto.getPk() / dto.getBeforeData() 处理删除
        } else {
            // dto.getAfterData() 为变更后整行镜像
        }
        log.info("binlog: {} -> {}, pk={}", dto.getTableName(), dto.getEventType(), dto.getPk());
    }
}
```

启动应用，看到以下日志即接入成功：

```
[blanka-cdc] infra resolved, canalServerAddr=..., destination=..., namesrvAddr=..., producerGroup=...
[blanka-cdc] DefaultMQProducer started, ...
[blanka-cdc] client started, thread=blanka-cdc-consume, ...
[blanka-cdc] connected to canal-server, addr=..., destination=...
```

### 3.3 完整配置项参考

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `blanka.cdc.enable` | `true` | 总开关；缺省即启用 |
| `blanka.cdc.canal-server-addr` | 空 | canal tcp 地址 `host:port`；为空时按 2.1 优先级补全 |
| `blanka.cdc.destination` | `example` | canal destination |
| `blanka.cdc.username` / `password` | 空 | canal 认证账号（canal-server 未开启认证则留空） |
| `blanka.cdc.namesrv-addr` | 空 | RocketMQ nameserver；为空时自动拾取 `rocketmq.name-server` |
| `blanka.cdc.producer-group` | 空 | producer group；为空时自动拾取 `rocketmq.producer.group` |
| `blanka.cdc.auto-route-enable` | `true` | 自动路由开关；关闭后未命中规则统一走 `default-topic` |
| `blanka.cdc.auto-topic-prefix` | `blanka_binlog_` | 自动路由 topic 前缀 |
| `blanka.cdc.default-topic` | `blanka_binlog_default` | 兜底 topic |
| `blanka.cdc.skip-ddl` | `true` | 跳过 DDL / 事务开始结束事件 |
| `blanka.cdc.batch-strategy` | `ROLLBACK_ALL` | `ROLLBACK_ALL` / `LOCAL_TABLE` |
| `blanka.cdc.exclude-patterns` | `[]` | 黑名单正则（匹配 `schema.table`），命中不投递 |
| `blanka.cdc.route-rules[].enable` | `true` | 规则开关 |
| `blanka.cdc.route-rules[].table-pattern` | — | 表匹配正则（匹配 `schema.table`） |
| `blanka.cdc.route-rules[].topic` | — | 目标 topic |
| `blanka.cdc.route-rules[].tag` | — | 消息 tag |

### 3.4 常用配置示例

**多表归集 / 自定义 topic（方式B：局部自定义）**

```yaml
blanka:
  cdc:
    auto-topic-prefix: "medical_blanka_"     # 调整自动路由前缀（公司级 topic 规范）
    skip-ddl: false
    exclude-patterns:                        # 黑名单：日志表、临时表不投递
      - "db_log\\..*"
      - "db_order\\.tmp_.*"
    route-rules:                             # 指定表路由到业务专属 topic
      - table-pattern: "db_order\\.order_pay"
        topic: "pay_topic"
        tag: "pay"
      - table-pattern: "db_order\\..*"      # 订单库其余表统一归集
        topic: "order_binlog_topic"
        tag: "order"
        enable: true
```

> 规则按 yaml 声明顺序匹配，先命中先得；`enable: false` 的规则在启动编译期即被剔除。

### 3.5 进阶：SPI 扩展

**场景一：公司统一配置中心下发基础设施地址（`InfraConfigProvider`）**

```java
public class CompanyInfraProvider implements InfraConfigProvider {

    @Override public String getCanalServerAddr() { return "canal.mycorp.com:11111"; }
    @Override public String getDestination()     { return "prod_instance"; }
    @Override public String getNamesrvAddr()     { return null; }  // 返回null表示跳过该项
    @Override public String getProducerGroup()   { return null; }
}
```

**场景二：自定义路由策略（`BlankaRouteStrategy`），如分库分表归一、按行值分流**

```java
public class ShardingRouteStrategy implements BlankaRouteStrategy {

    @Override
    public RouteResult route(String schemaName, String tableName, CanalEntry.RowChange rowChange) {
        // 分表 order_info_0 ~ order_info_99 统一路由到同一 topic
        if (tableName.startsWith("order_info_")) {
            return new RouteResult("order_binlog_topic", "order", "SPI");
        }
        return null;  // 返回null：放弃本层，继续走 yaml 规则 → 自动路由
    }
}
```

两个接口的注册方式相同 —— 在自己工程的 `src/main/resources/META-INF/services/` 下新建文件：

```
# 文件名 = SPI 接口全限定名，内容 = 实现类全限定名
META-INF/services/com.blanka.cdc.autoconfigure.spi.InfraConfigProvider
    ↳ 一行: com.mycompany.CompanyInfraProvider

META-INF/services/com.blanka.cdc.autoconfigure.spi.BlankaRouteStrategy
    ↳ 一行: com.mycompany.ShardingRouteStrategy
```

实现类会被自动注册为 Spring Bean（可直接注入业务 Bean）；启动日志会打印
`spi implementation registered: ...` 便于确认。

### 3.6 `@EnableBlankaCdc` 注解（可选）

```java
@EnableBlankaCdc     // 可选：默认不写注解、enable 缺省 true 时同样自动装配
@SpringBootApplication
public class MyApp { ... }
```

仅在与其它 CDC 组件共存、需要**显式声明**开启时使用；日常接入无需添加。

---

## 四、构建与本地验证

```bash
# 根目录执行（需要 Java 17）
mvn clean package -DskipTests
```

构建产物：

- `blanka-cdc-autoconfigure/target/blanka-cdc-autoconfigure-1.0.0.jar`
- `blanka-cdc-spring-boot-starter/target/blanka-cdc-spring-boot-starter-1.0.0.jar`
- `blanka-cdc-demo/target/blanka-cdc-demo-1.0.0.jar`（可执行 jar，可直接 `java -jar` 启动验证）

demo 演示工程已通过端到端验证（配置补全、自动装配、producer 启动、消费线程生命周期）。

## 五、注意事项

1. **消费端必须幂等**：无论哪种 batch 策略，MQ 语义上均存在重复投递可能（`ROLLBACK_ALL` 整批重放、MQ 重试），推荐以 `schema.table.pk + batchId` 或业务版本号做幂等
2. **canal-server 是部署前提**：本 starter 只做"消费 canal → 投递 MQ"，MySQL 监听账号 / binlog 解析 / 位点持久化由 canal-server 负责
3. **topic 需预先创建或开启自动创建**：自动路由生成的 topic 若 broker 未开启 `autoCreateTopicEnable`，需运维预建
4. **`LOCAL_TABLE` 策略现状**：V1.0 为吞吐优先骨架（先 ack 后发送，失败仅日志），V1.2 将补齐本地投递表 + 重试死信；当前使用请确保消费端幂等
5. **默认订阅范围为 canal-server 侧配置的库表**：客户端订阅 `.*\\..*`，实际投递范围 = canal-server instance 配置的监听规则 ∩ starter 黑名单过滤

## 六、Roadmap

- V1.2：`LOCAL_TABLE` 本地投递表 + 重试死信队列
- V1.1：路由规则动态刷新（`compileRules()` 已预留）
