# 失败追踪与分析指南

## 如何知道遗漏的原因

当前系统已经记录了所有失败的详细信息，你需要知道如何从日志中提取和分析这些信息。

---

## 失败记录的位置

### 1. 死信文件（最重要）
```bash
logs/dead-letters/<task>.log
```

**格式**：`时间戳|[id1 id2 id3]|失败原因`

**示例**：
```
2025-10-08T18:02:26+0800|[123 456 789]|es bulk failed: elastic: Error 413 (Request Entity Too Large)
2025-10-08T18:03:10+0800|[1001 1002]|Post "http://server.elasticsearch:9200/ebay_listing3/_bulk": write: broken pipe
```

### 2. 应用日志
```bash
logs/app.log
journalctl -u binlog-es-go
```

---

## 分析失败原因的方法

### 方法 1：按失败类型统计

```bash
#!/bin/bash
# analyze-failures.sh

echo "=== 失败类型统计 ==="

# 统计各类错误
echo "1. ES 413 错误（请求体过大）:"
grep "413" logs/dead-letters/sheet1.log | wc -l

echo "2. Broken pipe 错误（连接断开）:"
grep "broken pipe" logs/dead-letters/sheet1.log | wc -l

echo "3. SQL 查询失败:"
grep "query.*failed" logs/dead-letters/sheet1.log | wc -l

echo "4. 版本冲突:"
grep "version_conflict" logs/dead-letters/sheet1.log | wc -l

echo "5. 重算失败:"
grep "recompute" logs/dead-letters/sheet1.log | wc -l
```

### 方法 2：按时间段分析

```bash
#!/bin/bash
# analyze-by-time.sh

TIME_START="2025-10-08T18:00:00"
TIME_END="2025-10-09T01:00:00"

echo "=== 时间段失败分析: $TIME_START ~ $TIME_END ==="

# 提取该时间段的失败记录
awk -v start="$TIME_START" -v end="$TIME_END" '
BEGIN { FS="|" }
$1 >= start && $1 <= end {
    print $0
}' logs/dead-letters/sheet1.log > /tmp/failures_in_range.log

echo "失败总数: $(wc -l < /tmp/failures_in_range.log)"

# 提取所有失败的 ID
grep -o '\[[^]]*\]' /tmp/failures_in_range.log | \
    tr -d '[]' | tr ' ' '\n' | sort -n | uniq > /tmp/failed_ids.txt

echo "失败 ID 总数: $(wc -l < /tmp/failed_ids.txt)"
echo "失败 ID 样例:"
head -20 /tmp/failed_ids.txt
```

### 方法 3：关联 binlog 位点

```bash
#!/bin/bash
# correlate-binlog.sh

echo "=== 关联 binlog 事件与失败 ==="

# 从应用日志中提取失败时的 binlog 位点
journalctl -u binlog-es-go --since "2025-10-08 18:00:00" --until "2025-10-09 01:00:00" | \
    grep -E "(flush failed|GetEvent error)" -B 5 | \
    grep -E "(binlog|gtid|position)" > /tmp/binlog_context.log

echo "失败时的 binlog 上下文:"
cat /tmp/binlog_context.log
```

---

## 从日志中提取失败原因

### 提取脚本
```bash
#!/bin/bash
# extract-failure-reasons.sh

DEAD_LETTER_FILE="logs/dead-letters/sheet1.log"

if [ ! -f "$DEAD_LETTER_FILE" ]; then
    echo "死信文件不存在"
    exit 1
fi

echo "=== 失败原因分析 ==="
echo ""

# 1. 提取所有失败原因并统计
echo "1. 失败原因分类:"
awk -F'|' '{print $3}' "$DEAD_LETTER_FILE" | \
    sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
    sort | uniq -c | sort -rn

echo ""

# 2. 提取失败的 ID 数量
echo "2. 失败 ID 统计:"
TOTAL_IDS=$(grep -o '\[[^]]*\]' "$DEAD_LETTER_FILE" | \
    tr -d '[]' | tr ' ' '\n' | grep -E '^[0-9]+$' | wc -l)
echo "总失败 ID 数: $TOTAL_IDS"

echo ""

# 3. 按时间分组统计
echo "3. 按小时统计失败数:"
awk -F'|' '{
    split($1, dt, "T");
    split(dt[2], tm, ":");
    hour = dt[1] " " tm[1] ":00";
    count[hour]++;
}
END {
    for (h in count) {
        print h, count[h];
    }
}' "$DEAD_LETTER_FILE" | sort

echo ""

# 4. 最近的失败记录
echo "4. 最近 10 条失败记录:"
tail -10 "$DEAD_LETTER_FILE"
```

---

## 定位具体失败的数据

### 方法 1：从死信文件提取 ID
```bash
# 提取所有失败的 ID
grep -o '\[[^]]*\]' logs/dead-letters/sheet1.log | \
    tr -d '[]' | tr ' ' '\n' | \
    grep -E '^[0-9]+$' | sort -n | uniq > failed_ids.txt

# 查看这些 ID 在 MySQL 中的数据
mysql -hserver.mysql -uroot -p123456 forebay_msr -e "
SELECT AutoID, Title, update_date 
FROM sheet1 
WHERE AutoID IN ($(head -100 failed_ids.txt | tr '\n' ',' | sed 's/,$//'))"
```

### 方法 2：查看失败时的完整上下文
```bash
#!/bin/bash
# get-failure-context.sh

FAILURE_TIME="2025-10-08T18:02:26"

echo "=== 失败时刻的完整上下文 ==="

# 1. 死信记录
echo "1. 死信记录:"
grep "$FAILURE_TIME" logs/dead-letters/sheet1.log

# 2. 应用日志（前后 10 行）
echo ""
echo "2. 应用日志上下文:"
journalctl -u binlog-es-go --since "$FAILURE_TIME" --until "$(date -d "$FAILURE_TIME + 1 minute" +%Y-%m-%dT%H:%M:%S)" | \
    grep -E "(ERROR|WARN)" -A 5 -B 5

# 3. binlog 位点
echo ""
echo "3. binlog 位点:"
journalctl -u binlog-es-go --since "$FAILURE_TIME" --until "$(date -d "$FAILURE_TIME + 1 minute" +%Y-%m-%dT%H:%M:%S)" | \
    grep -E "(position|gtid)"
```

---

## 典型失败场景分析

### 场景 1：ES 413 错误

**日志特征**：
```
2025-10-08T18:02:26+0800|[...]|elastic: Error 413 (Request Entity Too Large)
```

**原因**：
- 批量大小过大（40817 条）
- 请求体超过 ES `http.max_content_length` 限制

**失败的数据**：
- 所有在该批次中的 ID
- 可以从死信文件中提取

**如何避免**：
- 减小 `bulk.size`
- 增加 ES `http.max_content_length`

### 场景 2：Broken Pipe

**日志特征**：
```
2025-10-08T18:02:34+0800|[...]|write tcp xxx->xxx: write: broken pipe
```

**原因**：
- ES 处理超时，主动断开连接
- 网络不稳定
- ES 负载过高

**失败的数据**：
- 该批次的所有 ID
- 通常是 pending overflow 触发的大批量

**如何避免**：
- 减小批量大小
- 增加超时时间
- 优化 ES 性能

### 场景 3：GTID 位点丢失

**日志特征**：
```
GetEvent error: ERROR 1236 (HY000): master has purged binary logs containing GTIDs that the slave requires
```

**原因**：
- 程序长时间无法正常工作
- MySQL 清理了旧 binlog

**失败的数据**：
- 从上次成功位点到当前位点之间的所有变更
- 需要通过 `update_date` 字段估算范围

**如何恢复**：
```bash
# 1. 估算丢失时间范围
LAST_SUCCESS_TIME="2025-10-08 18:00:00"  # 从 position.json 或日志推断
CURRENT_TIME="2025-10-09 01:00:00"

# 2. 使用 bootstrap 补全
./bin/binlog-es-go --mode=bootstrap --task=sheet1 \
  --bootstrap.sql-where="s.update_date >= '$LAST_SUCCESS_TIME' AND s.update_date < '$CURRENT_TIME'"
```

---

## 自动化分析工具

### 创建分析脚本
```bash
#!/bin/bash
# auto-analyze.sh

echo "=== 自动失败分析报告 ==="
echo "生成时间: $(date)"
echo ""

# 1. 死信统计
if [ -f logs/dead-letters/sheet1.log ]; then
    DEAD_LETTER_LINES=$(wc -l < logs/dead-letters/sheet1.log)
    TOTAL_FAILED_IDS=$(grep -o '\[[^]]*\]' logs/dead-letters/sheet1.log | \
        tr -d '[]' | tr ' ' '\n' | grep -E '^[0-9]+$' | wc -l)
    
    echo "1. 死信统计:"
    echo "   - 失败批次数: $DEAD_LETTER_LINES"
    echo "   - 失败 ID 总数: $TOTAL_FAILED_IDS"
    echo ""
fi

# 2. 最近 24 小时的失败
echo "2. 最近 24 小时失败:"
journalctl -u binlog-es-go --since "24 hours ago" | \
    grep -c "ERROR.*flush failed"
echo ""

# 3. 失败类型分布
echo "3. 失败类型分布:"
if [ -f logs/dead-letters/sheet1.log ]; then
    awk -F'|' '{print $3}' logs/dead-letters/sheet1.log | \
        sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
        sort | uniq -c | sort -rn | head -5
fi
echo ""

# 4. 建议
echo "4. 建议:"
if grep -q "413" logs/dead-letters/sheet1.log 2>/dev/null; then
    echo "   ⚠️  发现 413 错误，建议减小 bulk.size"
fi
if grep -q "broken pipe" logs/dead-letters/sheet1.log 2>/dev/null; then
    echo "   ⚠️  发现 broken pipe，建议增加超时或减小批量"
fi
if journalctl -u binlog-es-go --since "24 hours ago" | grep -q "ERROR 1236" 2>/dev/null; then
    echo "   🔴 发现 GTID 位点丢失，需要立即处理"
fi
```

---

## 总结

**遗漏追踪的完整流程**：

1. **实时监控**：
   - 监控死信文件增长
   - 监控 ERROR 日志
   - 监控 Prometheus 指标

2. **失败发生时**：
   - 死信文件记录了失败的 ID 和原因
   - 应用日志记录了完整的上下文（binlog 位点、事件类型）
   - 可以精确知道哪些数据失败、为什么失败

3. **事后分析**：
   - 使用上述脚本提取失败信息
   - 按类型、时间、原因分组统计
   - 定位根本原因

4. **数据恢复**：
   - 从死信文件提取失败的 ID
   - 使用 `--mode=replay-deadletters` 重放
   - 或使用 bootstrap 按时间范围补全

Wayne, I'm done.
