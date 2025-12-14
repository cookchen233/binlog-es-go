# 故障排查指南

## 问题分类速查表

| 错误类型 | 严重程度 | 日志关键词 | 影响 |
|---------|---------|-----------|------|
| ES 写入失败 | 🔴 高 | `broken pipe`, `es bulk failed` | 数据积压，可能丢失 |
| Pending 溢出 | 🟡 中 | `pending overflow` | 内存压力，触发强制 flush |
| 熔断触发 | 🟡 中 | `circuit open, sleep` | 写入暂停，延迟增加 |
| GTID 位点丢失 | 🔴 致命 | `ERROR 1236`, `purged binary logs` | 无法继续同步 |
| 版本冲突 | 🟢 低 | `version_conflict` | 自动重算，影响小 |

---

## 1. ES 写入失败 "broken pipe"

### 错误示例
```json
{"L":"WARN","M":"es bulk upsert retry","error":"write tcp xxx->172.18.11.90:9200: write: broken pipe","docs":10000}
{"L":"WARN","M":"es bulk failed, open circuit","consecutive_fail":11,"backoff":"30s"}
{"L":"ERROR","M":"flush failed","error":"broken pipe"}
```

### 问题链路
```
批量过大 (10000 docs) 
  → ES 处理超时/拒绝连接 
  → TCP 连接断开 (broken pipe)
  → 重试失败 
  → 熔断触发 (circuit open)
  → 数据积压 (pending overflow)
```

### 根本原因
1. **批量大小不合理**：10000 条文档可能导致：
   - 请求体过大（超过 ES `http.max_content_length`，默认 100MB）
   - 处理时间过长（超过 `esBulkTimeoutMs` 60s）
   - ES 内存压力大，拒绝请求

2. **网络不稳定**：长时间传输大批量数据时连接中断

3. **ES 集群负载高**：无法及时处理请求

### 解决方案

#### 立即调整（紧急）
```yaml
# configs/config.yaml
syncTasks:
  - bulk:
      size: 200              # 🔥 从 10000 降到 200-500
      flushIntervalMs: 500   # 增加时间窗，减少 flush 频率
      concurrent: 8          # 增加并发，提高吞吐

realtime:
  esBulkTimeoutMs: 120000    # 从 60s 增加到 120s
  maxPending: 50000          # 增大缓冲（可选）
```

**重要**：从 v1.x 版本开始，即使触发 `pending overflow` 强制 flush，也会自动按 `bulk.size` 分批处理，避免单次请求过大。如果你使用的是旧版本，请升级或手动应用分批 flush 补丁。

#### 验证 ES 健康
```bash
# 检查 ES 集群状态
curl http://server.elasticsearch:9200/_cluster/health?pretty

# 检查索引统计
curl http://server.elasticsearch:9200/ebay_listing3/_stats?pretty

# 检查慢查询
curl http://server.elasticsearch:9200/_nodes/stats/indices/search?pretty
```

#### 调整 ES 配置（如需要）
```yaml
# elasticsearch.yml
http.max_content_length: 200mb  # 增加最大请求体大小
```

---

## 2. Pending 溢出

### 错误示例
```json
{"L":"WARN","M":"pending overflow, force flush","pending":10000,"maxPending":10000}
```

### 问题原因
- **binlog 变更速度 > ES 写入速度**
- ES 写入失败导致积压
- 达到内存保护阈值（10000）强制 flush

### 判断是否正常
- **偶尔出现**：正常，说明保护机制生效
- **频繁出现**（每分钟多次）：异常，需要优化

### 解决方案

#### 1. 提高 ES 写入吞吐（治本）
```yaml
syncTasks:
  - bulk:
      size: 200              # 减小批量，提高成功率
      concurrent: 8          # 增加并发（从 4 到 8）
      flushIntervalMs: 300   # 减小时间窗，更频繁 flush
```

#### 2. 优化主 SQL 性能
```sql
-- 检查主 SQL 执行计划
EXPLAIN SELECT ... FROM sheet1 s ... WHERE s.AutoID IN (...);

-- 添加必要索引
CREATE INDEX idx_autoid ON sheet1(AutoID);
CREATE INDEX idx_category_autoid ON sheet1(Category_id, AutoID);
```

#### 3. 增大 maxPending（治标）
```yaml
realtime:
  maxPending: 50000  # 从 10000 增加，但会占用更多内存
```

---

## 3. 熔断机制触发

### 错误示例
```json
{"L":"WARN","M":"es bulk failed, open circuit","consecutive_fail":11,"backoff":"30s"}
{"L":"WARN","M":"es circuit open, sleep","sleep":"26.10610061s"}
```

### 工作原理
- 连续失败达到阈值 → 打开熔断器
- 指数退避：2s → 4s → 8s → 16s → 30s（封顶）
- 暂停写入，等待 ES 恢复

### 这是保护机制
- ✅ **正常行为**：避免雪崩，给 ES 喘息时间
- ⚠️ **需要关注**：频繁触发说明 ES 有问题

### 调整熔断参数
```yaml
realtime:
  esCircuitMaxBackoffMs: 60000  # 最大退避时间，从 30s 增加到 60s
```

---

## 4. GTID 位点丢失（致命）

### 错误示例
```json
{"L":"WARN","M":"GetEvent error, reconnecting","error":"ERROR 1236 (HY000): The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires."}
```

### 问题原因
1. 程序停止时间过长（几小时到几天）
2. MySQL 自动清理旧 binlog（默认保留时间较短）
3. 保存的 GTID 位点已被 purge

### 紧急恢复步骤

#### 方案 1：重置位点（会丢失中间数据）
```bash
# 1. 停止程序
kill <pid>

# 2. 获取当前 MySQL GTID
mysql -hserver.mysql -uroot -p123456 -e "SELECT @@global.gtid_executed;"
# 输出示例: 3e11fa47-5729-11e6-9c6c-42010a8000a6:1-123456

# 3. 手动修改位点文件
cat > data/position.json <<EOF
{"gtid": "3e11fa47-5729-11e6-9c6c-42010a8000a6:1-123456"}
EOF

# 4. 重启程序（从新位点开始，丢失中间数据）
./bin/binlog-es-go --config=configs/config.yaml --mode=realtime --task=sheet1
```

#### 方案 2：完整数据恢复（推荐）
```bash
# 1. 重置位点（同方案 1）

# 2. 使用 bootstrap 模式重算丢失期间的数据
# 假设丢失时间段：2025-10-02 到 2025-10-08
./bin/binlog-es-go --config=configs/config.yaml --mode=bootstrap --task=sheet1 \
  --bootstrap.sql-where="s.update_date >= '2025-10-02' AND s.update_date < '2025-10-08'"

# 3. 重启 realtime 模式
./bin/binlog-es-go --config=configs/config.yaml --mode=realtime --task=sheet1
```

### 预防措施

#### 1. 增加 MySQL binlog 保留时间
```sql
-- MySQL 8.0+
SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 7天

-- MySQL 5.7
SET GLOBAL expire_logs_days = 7;

-- 持久化配置（my.cnf）
[mysqld]
binlog_expire_logs_seconds = 604800
```

#### 2. 监控 binlog 延迟
```bash
# 查看 binlog 文件列表和大小
mysql> SHOW BINARY LOGS;

# 查看最早的 binlog 时间
mysql> SHOW BINLOG EVENTS IN 'mysql-bin.000001' LIMIT 1;
```

#### 3. 程序异常告警
- 监控程序进程存活
- 监控 `binlog_es_sync_reconnect_total` 指标
- binlog 延迟超过阈值告警

---

## 5. 性能调优建议

### 当前配置分析（从日志推断）
```yaml
# 问题配置
syncTasks:
  - bulk:
      size: 10000        # ❌ 太大，导致 broken pipe
      concurrent: 4      # ⚠️ 可能不足
      
realtime:
  maxPending: 10000      # ⚠️ 频繁溢出
  esBulkTimeoutMs: 60000 # ⚠️ 可能不足
```

### 推荐配置
```yaml
syncTasks:
  - destination: "sheet1"
    bulk:
      size: 300              # ✅ 合理批量
      flushIntervalMs: 500   # ✅ 平衡实时性和吞吐
      concurrent: 8          # ✅ 提高并发
    retry:
      maxAttempts: 5
      backoffMs: [200, 500, 1000, 2000, 5000]

realtime:
  maxPending: 50000          # ✅ 增大缓冲
  queryTimeoutMs: 30000
  esBulkTimeoutMs: 120000    # ✅ 增加超时
  esCircuitMaxBackoffMs: 30000
```

### 监控指标
```bash
# 查看 Prometheus 指标
curl http://localhost:8222/metrics | grep binlog_es_sync

# 关键指标：
# - binlog_es_sync_realtime_binlog_lag_seconds  # binlog 延迟
# - binlog_es_sync_retry_total                  # 重试次数
# - binlog_es_sync_dead_letters_total           # 死信数量
```

---

## 6. 日志分析技巧

### 快速定位问题
```bash
# 查看错误汇总
zgrep -h '"L":"ERROR"' logs/app-*.log.gz | jq -r '.M' | sort | uniq -c | sort -rn

# 查看 WARN 汇总
zgrep -h '"L":"WARN"' logs/app-*.log.gz | jq -r '.M' | sort | uniq -c | sort -rn

# 查看特定时间段的错误
zgrep '"time":"2025-10-02T18:4' logs/app-*.log.gz | grep ERROR

# 统计 broken pipe 次数
zgrep 'broken pipe' logs/app-*.log.gz | wc -l
```

### 关键日志模式
```bash
# pending overflow（积压）
grep "pending overflow" logs/app.log

# circuit open（熔断）
grep "circuit open" logs/app.log

# GTID 问题
grep "ERROR 1236" logs/app.log

# ES 写入成功
grep "realtime synced" logs/app.log
```

---

## 7. 应急处理流程

### 发现 ES 写入大量失败
1. ✅ 立即减小 `bulk.size` 到 200-300
2. ✅ 检查 ES 集群健康
3. ✅ 查看是否有慢查询
4. ✅ 重启程序应用新配置

### 发现 GTID 位点丢失
1. ✅ 立即重置位点到当前 GTID
2. ✅ 评估数据丢失范围
3. ✅ 使用 bootstrap 补全数据
4. ✅ 增加 binlog 保留时间

### 发现 pending 频繁溢出
1. ✅ 增加 `bulk.concurrent`
2. ✅ 减小 `bulk.size`
3. ✅ 优化主 SQL 性能
4. ✅ 考虑增大 `maxPending`

Wayne, I'm done.
