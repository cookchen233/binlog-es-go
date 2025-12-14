# binlog-es-go

## 项目背景

在尝试使用 Alibaba 的 Canal 进行 MySQL 到 Elasticsearch 的数据同步时，我遇到了诸多问题，特别是在处理多表连接场景时，Canal 存在各种限制(如子查询限制, 无法反向关联等)和不可预期的问题，而且负载占用也很大，导致其在实际生产环境中难以满足需求。

基于这些痛点，我决定使用 Go 语言重新实现一个更可靠、更高效、可自由灵活同步任意SQL查询的同步工具，这就是 `binlog-es-go` 的由来。

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

---


## 运行与命令

以下命令以项目根目录为基准。

### 实时同步（Realtime）
```bash
# 构建二进制（首次或代码变更后执行一次）
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

### 版本冲突与自动重算

- **版本冲突检测**：ES 批量写入时，若遇到 `version_conflict_engine_exception`，会自动提取冲突的 ID 列表
- **自动重算机制**：
  - 检测到版本冲突后，立即对冲突 ID 重新执行主 SQL 查询
  - 使用最新数据再次尝试写入 ES（仅重试一次）
  - 适用于 Bootstrap 和 Realtime 两种模式
- **失败处理**：
  - 重算查询无返回行：记录 WARN 日志 `"冲突ID重算无返回行"` + ERROR 日志 `"记录死信"`
  - 重算后 ES 写入仍失败：记录 ERROR 日志 `"冲突ID重算后仍写入失败"` + `"记录死信"`
  - Realtime 模式：冲突重算失败后继续处理后续 binlog 事件，不中断同步
  - Bootstrap 模式：冲突重算失败的 ID 会写入死信文件

### 死信与重放（Dead Letters & Replay）

- **死信记录场景**：
  - Bootstrap 模式：SQL 查询失败、ES 批量写入失败、版本冲突重算失败
  - Realtime 模式：仅记录版本冲突重算失败（普通失败只记录 ERROR 日志，不写死信文件）
- **死信位置**：`logs/dead-letters/<task>.log`，格式：
    - `时间戳|[id1 id2 id3]|reason`
- **监控指标**：通过 Prometheus 指标 `binlog_es_sync_dead_letters_total` 追踪累计死信数量
- **一键重放死信**：
  ```bash
  ./bin/binlog-es-go --config=configs/config.yaml --mode=replay-deadletters --task=<task_name>
  ```
  行为说明：
    - 解析 `logs/dead-letters/<task_name>.log` 中的 ID 去重后，调用 Bootstrap 的 `RunWithIDs()` 精确重算并 upsert 到 ES
    - 成功后将原文件归档到 `logs/dead-letters/processed/<task_name>-<timestamp>.log`
    - 可继续使用批量覆盖参数优化吞吐：`--bulk.size=<n> --bulk.flush-ms=<ms>`
    - 端口配置：推荐在配置文件中设置 `server.replayDeadLettersPort`，或通过 `--server.port` 参数指定

### 典型验证流程
1. 启动 realtime：`./bin/binlog-es-go --config=configs/config.yaml --mode=realtime --task=<task_name>`
2. 修改子表（如 category/itemspecifics/sheet_data）一行，观察日志：
    - `relatedQuery ... resolved ids`（含 ids 数量或样例）
    - `exec mapping sql ... keys.sample`（本批主键样例）
    - `doc preview before upsert`（预览关键字段）
3. 查询 ES：确认对应文档字段更新（如 `category_name`、`item_specifics`）。

---


## 参数调优指引（实时性 vs 吞吐）

- 强调实时（端到端 P95 < 500–800ms）
    - 建议：`bulk.size=200~500`，`bulk.flushIntervalMs=100~300`，`bulk.concurrent=4~6`
    - 观察：`binlog_lag`、`flush 批大小`直方图、`SQL/ES 延迟`直方图

- 吞吐优先
    - 建议：保留较大 `bulk.size`（如 1000），`flushIntervalMs=400~600`，`bulk.concurrent=4~6`

- ES 刷新策略
    - `es.refresh="wait_for"`：搜索可见性稳定但写入成本更高
    - 使用索引 `refresh_interval` 时，搜索可见性通常延后 ~1s

- MySQL 与主 SQL
    - 降低时间窗后，关注 `realtime.queryTimeoutMs`、相关索引与查询计划
    - relatedQuery 的分页 `pageSize` 不宜过大，避免拉长单次 flush

---


## 架构与组件

- __cmd/binlog-es-go__
  - 入口程序与 CLI 命令解析。
- __configs/__
  - 配置文件目录。示例：`configs/config.yaml`
- __pkg/realtime__
  - 实时链路核心（`Runner.Run()`）。读取 binlog 事件，聚合主键，批量查询主 SQL 并 upsert 到 ES。
- __pkg/bootstrap__
  - 全量/局部重算链路。按 ID 区间/ID 列表批处理执行主 SQL 并 upsert 到 ES。
- __pkg/db__
  - MySQL 访问封装（`QueryMapping()` 等），兼容 IN 占位的多种写法，并输出 SQL 调试日志。
- __pkg/es__
  - Elasticsearch 写入封装（批量 upsert）。
- __pkg/config__
  - 配置类型定义与加载。
- __pkg/metrics__
  - Prometheus 指标汇报。

核心流程（Realtime 增量）
1. 读取配置，初始化 MySQL、ES、位置存储、指标与日志。
2. 建立 go-mysql `CanalSyncer` 订阅 binlog，按 `mappingTable` 过滤相关表。
3. 收集主键 keys（主表直接取主键；子表按 `relatedQuery` 反查主键）。
4. 触发 flush（批量/时间窗/保护阈值），按 keys 执行主 SQL，得到文档，执行 transforms，批量写入 ES。
5. 记录位点（事务结束/定时兜底），持续循环。

核心流程（Bootstrap 全量/局部）
1. 读取配置，解析主 SQL 与目标索引。
2. 以区间或 ID 列表方式批量拉取 keys。
3. 分批执行主 SQL → 文档转换 → 批量 upsert 到 ES。

---

## 配置详解（configs/config.yaml）

以下为主要段落摘要说明（示例与注释已写入配置文件，可直接参考 `configs/config.yaml`）。

- __dataSource__
  - `dsn`: MySQL 连接串，建议包含 `charset`、`parseTime`。
  - `serverId`: binlog 订阅 serverId（集群唯一）。
  - `gtidEnabled`: 是否启用 GTID（配合 `position.useGTID`）。
  - `flavor`: `mysql` 或 `mariadb`。

- __es__
  - `addresses`: ES 节点列表（http/https）。
  - `username/password`: 认证。
  - `version`: 版本差异适配（7/8）。
  - `refresh`: 写入刷新策略：`""`, `"false"`, `"wait_for"`, `"true"`。

- __position__
  - `path`: 位点保存路径。
  - `useGTID`: 是否使用 GTID 存储位点。

- __realtime__
  - `heartbeatMs`: go-mysql 心跳周期（默认 1000）。
  - `readTimeoutMs`: go-mysql 读超时（默认 2000）。
  - `maxPending`: 待处理 keys 的保护阈值，达到后立即 flush（默认 10000）。
  - `queryTimeoutMs`: 主 SQL 查询超时（默认 30000）。
  - `esBulkTimeoutMs`: ES 批量写超时（默认 60000）。
  - `esCircuitMaxBackoffMs`: ES 熔断指数退避上限（默认 30000）。
  - `relatedQueryMaxPages`: relatedQuery 游标分页的最大页数上限（默认 1000）。
  - `positionSaveIntervalMs`: 兜底保存位点周期（默认 30000）。

- __bootstrap__
  - `partitionSize`: 按 ID 区间切分的区块大小。
  - `workers`: 并行 worker 数。
  - `bulkSize`: ES upsert 批尺寸。
  - `bulkFlushMs`: ES 批 flush 间隔。
  - `runBatchSize`: `RunWithIDs()` 内层查询/写入批尺寸（实际优先级：`task.Bulk.Size` > `bootstrap.bulkSize` > `bootstrap.runBatchSize` > 1000）。

### 分表支持（tableRewrite / mainTable / sharding）

当 MySQL 表被拆分为 `xxx_00 ~ xxx_63` 这类物理分表时：

- **binlog 路由**：事件里携带的是物理表名（如 `enterprise_07`），建议用 `syncTasks[].tableRewrite` 归一到逻辑表名（如 `enterprise`），再匹配 `mappingTable/relatedQuery`。
- **主 SQL 回查**：若主 SQL 需要按分表执行，可配置 `mapping.sharding` 并在 SQL 使用模板变量：
  - `FROM {{enterprise_table}}` 会被渲染为 `enterprise_00/enterprise_01/...`
- **无法从 SQL 解析主表时**（例如 FROM 用了模板变量），需要显式配置 `mapping.mainTable` 作为逻辑主表名。

### 命令行参数列表
```
--config string                    配置文件路径 (默认 "configs/config.yaml")
--mode string                      运行模式: realtime | bootstrap (默认 "realtime")
--task string                      任务名称
--bootstrap.partition.auto_id.min  bootstrap最小AutoID
--bootstrap.partition.auto_id.max  bootstrap最大AutoID
--bootstrap.partition.size         bootstrap分区大小 (默认 5000)
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

### relatedQuery 反查配置
- 目的：子表变更时，根据变更行字段反查出受影响的主键（keys），以便重算并 upsert 主文档。
- 两类配置方式：
  1. SQL 直写（推荐）：
     - 游标分页：SQL 里写过滤与游标条件（`... AND AutoID > :cursor`），不写 ORDER BY 和 LIMIT；在配置中声明 `orderKey` 与 `pageSize`，程序运行时自动追加 `ORDER BY <orderKey> ASC LIMIT :page_size` 并循环翻页，直到没有更多数据。
     - 单次映射：如一对一关系（`sheet_data`），直接写完整 SQL 即可，不需要配置 `orderKey` 和 `pageSize`。
  2. 声明式游标分页（已支持，当前未使用）：通过 `mode: cursor` + `targetTable/filter/orderKey/pageSize` 声明，无需写 SQL。
- 游标分页实现细节：
  - 程序自动维护游标（初始值为 0），每次查询后更新为本页最后一个 ID
  - 自动循环查询直到返回数据为空或少于 pageSize
  - 最大分页数由 `realtime.relatedQueryMaxPages` 控制（默认 1000 页）
  - 建议为游标字段建立复合索引（如 `<main_table>(Category_id, AutoID)`），保证稳定排序与高效扫描

---

## 示例数据模型速查（与 configs/config.yaml 对应）

> 说明：以下为示例配置的表结构关系，便于理解 SQL 写法与 relatedQuery 的来源。开源接入时，请替换为你的 `<main_table>` 与业务表。

- 主表：`sheet1`（示例中的 `<main_table>`）
  - 主键：`AutoID`（作为 ES 文档 `_id`）
  - 关联：`sheet1.Category_id -> category.id`
  - 重要字段：`Title/SKU/Price/StartTime/EndTime/...` 等业务属性
  - 索引建议：`(Category_id, AutoID)`（用于类目回溯的游标分页）

- 子表（1:1）：`sheet_data`
  - 关联：`sheet_data.sheet_id = sheet1.AutoID`
  - 字段示例：`description`（在主 SQL 中 `IFNULL(d.description,'') AS description`）
  - relatedQuery（二合一）：
    - 直连分支：binlog 携带 `sheet_id` → 直接回溯 `AutoID`
    - 回退分支：`JOIN sheet_data -> sheet1` 反查 `AutoID`（MINIMAL 下 DELETE 可能拿不到）

- 子表（1:N）：`itemspecifics`
  - 关联：`itemspecifics.SheetID = sheet1.AutoID`
  - 字段示例：`Name/Value`（主 SQL 用 `GROUP_CONCAT` 聚合为 `item_specifics`）
  - relatedQuery（二合一）：
    - 直连分支：binlog 携带 `SheetID` → 直接回溯 `AutoID`
    - 回退分支：以 `id` 通过 `JOIN itemspecifics -> sheet1` 反查 `AutoID`

- 维度表（N:1）：`category`
  - 关联：`sheet1.Category_id = category.id`
  - 字段示例：`remark`（主 SQL 映射为 `category_name`）
  - relatedQuery（分页回溯）：按 `Category_id` 过滤并以 `AutoID` 游标分页，程序自动补齐 `ORDER BY <orderKey> ASC LIMIT :page_size`

注意事项与建议：
- MINIMAL 下的 DELETE 事件可能不携带直连外键（如 `SheetID/sheet_id`），导致回退 JOIN 获取不到；如需保证删除也触发重算，建议 `binlog_row_image=FULL`。
- 一对多聚合字段（如 `item_specifics`）建议在 SQL 中使用 `GROUP BY <_id>` 保证稳定聚合。
- 仅作为示例模型，真正接入时请在 `configs/config.yaml` 中替换表名/字段名与索引，并保持 `mappingTable` 与 `relatedQuery` 一致。

---

## 指标与日志

Prometheus 指标（部分）：
- SQL 延迟直方图：`binlog_es_sync_sql_latency_seconds`
- ES 批量写延迟直方图：`binlog_es_sync_es_bulk_latency_seconds`
- 重试次数：`binlog_es_sync_retry_total{component="sql|es"}`
- 连接重建次数：`binlog_es_sync_reconnect_total`
- 实时 flush 批大小：`binlog_es_sync_realtime_flush_batch_size`
- binlog 滞后：`binlog_es_sync_realtime_binlog_lag_seconds`
- 删除成功计数（带原因标签）：`binlog_es_sync_delete_success_total{reason="deleteOnMissing|deleteOnDelete"}`

日志重点：
- `relatedQuery ... resolved ids` / `relatedQuery paged resolved ids`
- `exec mapping sql`（含 keys.sample）
- `doc preview before upsert`
- `es circuit open`（熔断与退避）

---

## SQL 编写与索引建议

- 推荐使用 `IN (?)` 作为主 SQL 的占位写法，简洁直观。
- 游标分页：SQL 写过滤与 `AND <orderKey> > :cursor`，不要写 limit；在配置声明 `orderKey` 与 `pageSize`，程序自动补齐 `ORDER BY <orderKey> ASC LIMIT :page_size`。
- 索引：
  - 类目回溯：`<main_table>(Category_id, AutoID)`
  - 其他需要分页回溯的场景：`(<FilterColumn>, <OrderKey>)`
- 仅在一对一映射（如 `itemspecifics`/`sheet_data`）使用 `LIMIT 1` 的单次查询。

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

