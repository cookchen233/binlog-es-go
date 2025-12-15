# binlog-es-go

## 项目背景

在尝试使用 Alibaba 的 Canal 进行 MySQL 到 Elasticsearch 的数据同步时，我遇到了诸多问题，特别是在处理多表连接场景时，Canal 存在各种限制(如子查询限制, 无法反向关联等)和不可预期的问题，而且负载也很高，导致其在实际生产环境中难以满足需求。

基于这些痛点，我决定使用 Go 语言重新实现一个更可靠、更高效、可自由灵活同步任意SQL查询的同步工具，专注于将 MySQL 数据同步到 ES这一场景。

## 项目简介

`binlog-es-go` 是一个专为 MySQL 到 Elasticsearch 实时数据同步设计的高性能工具，特别针对多表关联、复杂查询等场景进行了优化。

## 核心特性

- **实时增量同步**：基于 MySQL binlog 实现毫秒级数据同步
- **全量/局部重算**：支持按需进行全量或局部数据重算（Bootstrap）
- **灵活映射**：提供强大的 SQL 映射能力，支持复杂查询和字段转换
- **关联查询**：支持子表到主表的反查（relatedQuery）
- **完善监控**：内置可观测性指标，实时监控同步状态
- **高度可配置**：丰富的配置项，满足不同业务场景需求

新增（分表/分库场景）
- **表名归一（tableRewrite）**：将 binlog 事件中的物理表名（如 `enterprise_00`）重写为逻辑表名（如 `enterprise`），使任务配置保持稳定
- **分表路由（mapping.sharding）**：按主键取模路由到物理分表，并对 SQL 中的 `{{var}}` 模板变量做渲染

新增（Elasticsearch 兼容性）
- **ES 7/8/9 兼容**：写入侧基于标准 HTTP REST API（`/_bulk`、`/_cluster/health`、`HEAD /{index}`）
- **HTTPS/TLS 支持**：当 `es.addresses` 使用 `https://` 时，可通过 `es.tls` 配置自签 CA / mTLS / 跳过校验（仅开发环境）

---


## 运行与命令

以下命令以项目根目录为基准。

### 实时同步（Realtime）
```bash
# 构建二进制
go build -o bin/binlog-es-go ./cmd/binlog-es-go

# 指定配置与任务名，启动实时链路
# mode=realtime 会启动 binlog 订阅、HTTP 服务(/healthz, /metrics)
./bin/binlog-es-go --config=configs/config.yaml --mode=realtime --task=<task_name>
```

健康检查与指标：
- `http://127.0.0.1:8222/healthz` 返回 OK
- `http://127.0.0.1:8222/metrics` Prometheus 指标

常见可调参数（`configs/config.yaml`）：
- `syncTasks[].bulk.size`、`syncTasks[].bulk.flushIntervalMs`、`syncTasks[].bulk.concurrent`
- `realtime.maxPending`、`realtime.queryTimeoutMs`、`realtime.esBulkTimeoutMs`

MySQL 8.4 注意事项
- 若未配置/保存 file+pos 且未启用 GTID，程序会尝试读取 binlog 当前起点。
  - 旧版本 MySQL 常用：`SHOW MASTER STATUS`
  - MySQL 8.4 推荐：`SHOW BINARY LOG STATUS`
- 运行账号需要具备 binlog 相关权限（至少 `REPLICATION CLIENT`，订阅 binlog 通常还需 `REPLICATION SLAVE`）。

### 全量/局部重算（Bootstrap）
```bash
# 全量扫描（推荐）
# - 默认不需要提供 min/max，适用于雪花ID/非自增主键（使用游标翻页扫描：ORDER BY 主键 LIMIT pageSize）
# - 若配置了 mapping.sharding.shards，会自动枚举物理分表（如 enterprise_00..enterprise_63）并并发扫描
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name>

# 按 ID 区间重算
# 例如：将 [100000, 200000) 范围内的文档重算后回写 ES
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name> \
  --bootstrap.partition.auto_id.min=100000 --bootstrap.partition.auto_id.max=200000

# 按 WHERE 条件重算（更灵活的方式）
# 示例：重算特定ID
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name> \
  --bootstrap.sql-where="s.AutoID = 305681"

# 重算多个ID
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=<task_name> \
  --bootstrap.sql-where="s.AutoID IN (305681,305682,305700)"
```

并发与批量参数由 `configs/config.yaml` 中 `bootstrap` 与 `syncTasks[].bulk` 决定：
- `bootstrap.partitionSize`、`bootstrap.workers`、`bootstrap.bulkSize`、`bootstrap.bulkFlushMs`、`bootstrap.runBatchSize`
- `syncTasks[].bulk.size`（如未显式指定，会用于 `RunWithIDs()` 的内层批尺寸优先级判断）

说明：
- 不传 `bootstrap.partition.auto_id.min/max` 时，`bootstrap.partitionSize` 表示全量扫描时的 `pageSize`（每页拉取多少个主键）。
- 传了 `bootstrap.partition.auto_id.min/max` 时，`bootstrap.partitionSize` 仍按旧语义作为区间扫描的分区大小。

### deleteOnMissing：语义与使用建议

`mapping.deleteOnMissing` 的语义是：**当主 SQL 对传入的一批 keys 回查无结果时，将这些 _id 对应的 ES 文档删除**。

- 适用场景：主表物理删除、或主 SQL 本身带过滤条件（不满足条件时文档应从 ES 消失）。
- 风险：如果业务写入存在“乱序/延迟落库”（例如导入时子表先写入、主表稍后写入），会在短时间窗口内出现“回查无结果”，从而误删 ES。
- 建议：
  - 仅当你确信“回查无结果 == 文档应消失”时才开启。
  - 否则推荐保持 `deleteOnMissing=false`，并只启用 `deleteOnDelete=true`（主表 delete 事件删除 ES）。

## 配置详解（configs/config.yaml）

配置项较多，建议直接参考：

- `configs/config.yaml`（带注释的真实示例）
- `configs/config.example.yaml`（最小化模板）

最常用的字段：

- `dataSource.dsn`
- `es.addresses`
- `syncTasks[].mapping`（`_index/_id/sql/deleteOnDelete/deleteOnMissing/mainTable/sharding`）
- `syncTasks[].mappingTable` / `syncTasks[].relatedQuery`

### 分表支持（tableRewrite / mainTable / sharding）

当 MySQL 表被拆分为 `xxx_00 ~ xxx_63` 这类物理分表时：

- **binlog 路由**：事件里携带的是物理表名（如 `enterprise_07`），建议用 `syncTasks[].tableRewrite` 归一到逻辑表名（如 `enterprise`），再匹配 `mappingTable/relatedQuery`。
- **主 SQL 回查**：若主 SQL 需要按分表执行，可配置 `mapping.sharding` 并在 SQL 使用模板变量：
-  - `FROM {{enterprise_table}}` 会被渲染为 `enterprise_00/enterprise_01/...`
-  - 子查询/关联表同样需要使用模板变量（否则会去查逻辑表名导致 `Table ... doesn't exist`），例如：`FROM {{enterprise_event_table}}`
- **无法从 SQL 解析主表时**（例如 FROM 用了模板变量），需要显式配置 `mapping.mainTable` 作为逻辑主表名。

#### 分表路由公约（sharding.strategy）

为保证跨语言一致性，推荐统一遵守以下约定：

- 分片 key 统一按十进制字符串处理（例如 `258652761531355136`），以 UTF-8 bytes 作为 hash 输入。
- 默认策略：`crc32_ieee_uint32`（无符号 CRC32 IEEE），分片计算：`crc32_ieee_uint32(utf8(key_string)) % shards`。

支持的 `sharding.strategy` 值：

- `crc32_ieee_uint32`：默认策略。
- `mod`：`abs(key) % shards`。
- `crc32_ieee_signed_abs`：兼容策略，`abs(int32(crc32_ieee_uint32(bytes))) % shards`。
- `crc32`：兼容别名，等价于 `crc32_ieee_signed_abs`（仅用于兼容历史配置，不推荐新项目使用）。

测试向量（`shards=64`）：

- `key="0"` -> shard `33`
- `key="1"` -> shard `55`
- `key="42"` -> shard `08`
- `key="258652761531355136"` -> shard `00`
- `key="258652848596717568"` -> shard `63`

补充说明（重要）：mappingTable 与 relatedQuery 的职责

- `mappingTable` 的作用是：当某张表发生 binlog 事件时，**从该事件行里直接提取“主文档 _id”**（也就是主表的主键）用于触发重算/回写。
- 因此在“主表/子表”模型中：
  - 主表（如 `enterprise`）通常可配置 `mappingTable.enterprise: ["id"]`
  - 子表（如 `enterprise_event`、`enterprise_contact` 等）如果自身也有 `id` 主键，**不要**配置成 `mappingTable.<child>: ["id"]`，否则程序会把“子表 id”误当作“主表 id”，导致主 SQL 回查无结果。
- 子表变更应使用 `relatedQuery`：通过子表行中的外键（如 `enterprise_id`）回溯得到主表主键集合，再触发主 SQL 重算。
- 若同时开启 `mapping.deleteOnMissing: true`，上述误配会进一步导致误删 ES 文档（因为回查无结果会被当成“应删除”）。

### 命令行参数列表
```
--config string                    配置文件路径 (默认 "configs/config.yaml")
--mode string                      运行模式: realtime | bootstrap (默认 "realtime")
--task string                      任务名称
--bootstrap.partition.auto_id.min  bootstrap最小AutoID
--bootstrap.partition.auto_id.max  bootstrap最大AutoID
--bootstrap.partition.size         bootstrap参数: 全量扫描为 pageSize；区间扫描为分区大小 (默认 5000)
--bootstrap.workers                bootstrap工作线程数 (默认 4)
--bootstrap.sql-where              bootstrap额外WHERE条件
--bulk.size                        覆盖批量大小 (默认 1000)
--bulk.flush-ms                    覆盖批量刷新间隔ms (默认 800)
--log.level                        覆盖日志级别: debug|info|warn|error
--log.file                         覆盖日志文件路径
```

- __logging__
  - `level`: `debug|info|warn|error`。
  - `file`: 日志文件路径（留空仅控制台）。

- __debug__
  - `sql`: 打印最终 SQL 形态（含 IN 展开）。
  - `sqlParams`: 打印参数样例（谨慎开启）。

- __server__
  - `port`: 内置 HTTP 端口（/healthz, /metrics）。

- __syncTasks[]__
  - `destination`: 任务名（与 canal destination 等对应）。
  - `mapping`:
    - `_index`: ES 索引名。
    - `_id`: 文档 ID 字段。
    - `upsert`: 是否 upsert。
    - `deleteOnMissing`: 布尔。主 SQL 对输入 keys 查询无结果时，删除这些 ID 对应的 ES 文档（适合主表物理删除、或过滤条件不再满足导致文档应消失的场景）。
    - `deleteOnDelete`: 布尔。主表捕获到 binlog 的 DELETE_ROWS 事件时，直接删除对应 ID 的 ES 文档（仅主表事件生效，子表删除仍走重算）。
    - `sql`: 主 SQL（推荐 `IN (?)`；兼容 `IN(:keys)` / `IN(:auto_ids)` / 任意 `IN(:placeholder)`）。
  - `mappingTable`: 表→主键列（用于 event 解析与 related 映射）。
  - `relatedQuery`: 子表→主表反查配置（详见下文）。
  - `transforms`: 文档入 ES 前的转换（如 `splitFields`）。
  - `bulk`: 实时链路 flush 策略（批量/时间窗/并发）。
  - `retry`: SQL/ES 写的重试策略（最大次数+退避数组）。


---

## 常见问题（FAQ）

- __为什么要配置 pageSize？__
  - 多(主表)对一(从表)关系中，从表变更可能引发主表海量回溯。通过 `relatedQuery.pageSize` 游标分页分批处理，结合稳定的 `orderKey`，可避免跳页、重复与长时间阻塞。

- __如何避免一次变更引发海量回溯？__
  - 通过多层保护：`realtime.maxPending`（内存保护，触顶即 flush）、`relatedQuery.pageSize`（分批回溯）、`realtime.relatedQueryMaxPages`（安全页数上限）、合理的 `bulk.size/flushIntervalMs`（降低 ES 压力）。

- __`relatedQuery` 的 orderKey 必须是主键吗？__
  - 推荐使用严格递增且唯一的主键，如需按维度列回溯，建议建立 `(维度列, orderKey)` 复合索引以保证顺序扫描效率。

- __需要哪些索引？__
  - 类目回溯：`sheet1(Category_id, AutoID)`；
  - 其他分页回溯：`(<FilterColumn>, <OrderKey>)`；
  - 主 SQL 关联字段需要常规业务索引以避免 Hash Join 回表放大。

- __ONLY_FULL_GROUP_BY 会影响聚合吗？__
  - 我们在主 SQL 末尾增加了 `GROUP BY s.AutoID` 来保证聚合稳定与兼容；确保线上开启该 SQL 模式时不会报错。

- __如何设置 ES 刷新策略？__
  - 在 `es.refresh` 配置中可选 `""`（按索引 refresh_interval）、`"false"`、`"wait_for"`、`"true"`。若未设置，将按索引默认策略。

- __如何选择合适的批量与时间窗？__
  - 增大 `bulk.size`、`bulk.flushIntervalMs` 提高吞吐，可能增加端到端延迟；减小则能更快可见但对 ES QPS 压力更大。可结合 `binlog_lag`、ES 延迟指标观测调参。

- __失败与重试如何处理？__
  - SQL/ES 写均有 `retry` 配置，失败按退避数组重试。ES 写连续失败会打开熔断（指数退避，`realtime.esCircuitMaxBackoffMs` 封顶）
  - 版本冲突自动重算：检测到 `version_conflict_engine_exception` 时，自动对冲突 ID 重新查询并重试写入（仅一次）
  - 死信记录：Bootstrap 模式的所有失败和 Realtime 模式的冲突重算失败会写入 `logs/dead-letters/<task>.log`，可通过 `--mode=replay-deadletters` 重放

- __ES 写入频繁失败 "broken pipe" 或 "413 Request Entity Too Large" 怎么办？__
  - **根本原因**：
    - 批量过大（10000+ 条）导致 ES 处理超时或网络连接断开
    - `pending overflow` 强制 flush 时一次性写入数万条数据，超过 ES 限制
  - **解决方案**：
    1. **减小批量大小**（推荐 200-500）：
       ```yaml
       syncTasks:
         - bulk:
             size: 300              # 从 10000 降到 300
             flushIntervalMs: 500   # 增加时间窗，减少 flush 频率
       ```
    2. **增加 ES 超时**：
       ```yaml
       realtime:
         esBulkTimeoutMs: 120000  # 从 60s 增加到 120s
       ```
    3. **检查 ES 集群健康**：确保 ES 负载正常，无慢查询
    4. **调整 ES http.max_content_length**（如果批量体积过大）：
       ```yaml
       # elasticsearch.yml
       http.max_content_length: 200mb  # 默认 100mb
       ```
  - **注意**：程序已优化，即使 `pending overflow` 也会自动分批处理，避免单次请求过大

- __Pending 溢出告警频繁怎么办？__
  - **原因**：binlog 变更速度 > ES 写入速度，积压达到 10000 触发保护
  - **解决方案**：
    1. **提高 ES 写入吞吐**：减小 `bulk.size`，增加 `bulk.concurrent`
       ```yaml
       syncTasks:
         - bulk:
             size: 200
             concurrent: 8  # 从 4 增加到 8
       ```
    2. **增大 maxPending**（治标）：
       ```yaml
       realtime:
         maxPending: 50000  # 从 10000 增加，但会占用更多内存
       ```
    3. **优化主 SQL 性能**：添加索引，减少 JOIN 复杂度

- __GTID 位点丢失 (ERROR 1236) 怎么办？__
  - **错误**：`master has purged binary logs containing GTIDs that the slave requires`
  - **原因**：程序停止时间过长，MySQL 清理了旧 binlog
  - **紧急恢复**：
    1. 重置位点到当前 GTID：
       ```bash
       # 获取当前 MySQL GTID
       mysql> SELECT @@global.gtid_executed;
       
       # 手动修改 data/position.json
       {"gtid": "<当前 gtid_executed 值>"}
       
       # 重启程序（会从新位点开始，丢失中间数据）
       ```
    2. **完整数据恢复**：使用 bootstrap 模式重算丢失期间的数据
  - **预防措施**：
    1. 增加 MySQL binlog 保留时间：
       ```sql
       SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 7天
       ```
    2. 监控 binlog 延迟，及时处理程序异常

- __位点如何保证？__
  - 事务成功写入后持久化位点；同时周期性兜底保存（`realtime.positionSaveIntervalMs`）。支持 GTID 或文件位置两种方式（配置 `position.useGTID`）。

- __如何最小化 SQL 日志噪声？__
  - `debug.sql` 控制是否打印 SQL；`debug.sqlParams` 控制是否打印参数样例。日志内的 SQL 已做单行压缩，不会再出现多行大段 SQL。

- __如何进行局部回溯或单条验证？__
  - 使用 Bootstrap：`./bin/binlog-es-go --mode=bootstrap --task=<task_name> --bootstrap.partition.auto_id.min=<id> --bootstrap.partition.auto_id.max=<id+1>` 或使用 `--bootstrap.sql-where="s.AutoID = <id>"`。

- __如何知道数据遗漏的原因？__
  - **死信文件**：所有失败的 ID 和原因都记录在 `logs/dead-letters/<task>.log`
    - 格式：`时间戳|[id1 id2 id3]|失败原因`
    - 包含完整的错误信息（ES 413、broken pipe、SQL 失败等）
  - **分析工具**：使用 `./scripts/analyze-failures.sh <task>` 自动分析
    - 按失败类型统计（ES 413、broken pipe、版本冲突等）
    - 按时间分布分析（找出失败高峰期）
    - 提取失败的 ID 列表
    - 提供恢复建议
  - **应用日志**：包含失败时的 binlog 位点、事件类型等上下文
  - 详见 `docs/failure-analysis.md`

- __是否支持删除同步？__
  - 已支持按需删除：
    - 开启 `mapping.deleteOnDelete: true` 时，主表 DELETE_ROWS 事件会触发对该 `_id` 的 ES 删除；
    - 开启 `mapping.deleteOnMissing: true` 时，flush 执行主 SQL 若对输入 keys 返回空结果，会删除这些 `_id` 在 ES 中的文档；
  - 二者可独立启用。子表删除仍走重算（relatedQuery → upsert），不直接触发主文档删除。

- __时区与字符集设置有讲究吗？__
  - 建议在 DSN 中显式设定 `charset=utf8mb4`、`parseTime=true`；确保应用与数据库时区一致，避免时间字段偏移。

- __如何接入 Prometheus/Grafana？__
  - 进程会在 `server.port`（默认 8222）暴露 `/metrics`，可直接配置 Prometheus 抓取并在 Grafana 里构建面板。

- __生产环境日志建议？__
  - 建议 `logging.level=info` 或 `warn`，仅在排障时开启 `debug`；`debug.sqlParams` 默认关闭以避免日志过长与敏感信息暴露。

- __多模式同时运行时端口冲突怎么办？__
  - 默认情况下，所有模式都会启动 HTTP 服务器（用于 `/healthz` 和 `/metrics`）
  - 但实际上只有 **realtime 模式需要长期监控**，bootstrap/replay-deadletters 是短期任务
  - 有三种方式避免端口冲突：
    1. **禁用短期任务的 HTTP 服务器**（最简单，推荐）：
       ```yaml
       server:
         port: 8222
         disableForShortTasks: true  # bootstrap/replay-deadletters 不启动 HTTP 服务器
       ```
    2. **为不同模式配置独立端口**：
       ```yaml
       server:
         realtimePort: 8222            # realtime 模式端口
         bootstrapPort: 8223           # bootstrap 模式端口
         replayDeadLettersPort: 8224   # replay-deadletters 模式端口
       ```
    3. **命令行参数方式**：运行时通过 `--server.port=<port>` 覆盖
       ```bash
       ./bin/binlog-es-go --mode=replay-deadletters --task=sheet1 --server.port=8223
       ```
  - **端口选择优先级**：命令行参数 > 模式专用端口 > 通用端口 > 默认值 8222

