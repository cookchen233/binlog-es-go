#!/bin/bash
# 失败分析工具 - 分析死信和日志，找出遗漏的原因

set -e

TASK=${1:-sheet1}
DEAD_LETTER_FILE="logs/dead-letters/${TASK}.log"

echo "========================================="
echo "  失败分析报告 - Task: $TASK"
echo "  生成时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================="
echo ""

# 检查死信文件是否存在
if [ ! -f "$DEAD_LETTER_FILE" ]; then
    echo "✅ 没有发现死信文件，系统运行正常"
    exit 0
fi

# 1. 总体统计
echo "## 1. 总体统计"
echo "----------------------------------------"
TOTAL_BATCHES=$(wc -l < "$DEAD_LETTER_FILE")
echo "失败批次数: $TOTAL_BATCHES"

# 提取所有 ID
TOTAL_IDS=$(grep -o '\[[^]]*\]' "$DEAD_LETTER_FILE" | \
    tr -d '[]' | tr ' ' '\n' | grep -E '^[0-9]+$' | wc -l)
echo "失败 ID 总数: $TOTAL_IDS"

# 计算平均每批失败数
if [ $TOTAL_BATCHES -gt 0 ]; then
    AVG_PER_BATCH=$((TOTAL_IDS / TOTAL_BATCHES))
    echo "平均每批失败: $AVG_PER_BATCH 条"
fi
echo ""

# 2. 失败类型分析
echo "## 2. 失败类型分析"
echo "----------------------------------------"
awk -F'|' '{
    reason = $3;
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", reason);
    
    # 分类
    if (reason ~ /413|Request Entity Too Large/) {
        type = "ES_413_请求体过大";
    } else if (reason ~ /broken pipe/) {
        type = "网络_Broken_Pipe";
    } else if (reason ~ /version_conflict/) {
        type = "版本冲突";
    } else if (reason ~ /query.*failed|sql.*failed/) {
        type = "SQL查询失败";
    } else if (reason ~ /recompute.*failed/) {
        type = "重算失败";
    } else if (reason ~ /timeout/) {
        type = "超时";
    } else {
        type = "其他";
    }
    
    count[type]++;
}
END {
    for (t in count) {
        printf "%-25s : %d 次\n", t, count[t];
    }
}' "$DEAD_LETTER_FILE" | sort -t':' -k2 -rn
echo ""

# 3. 时间分布分析
echo "## 3. 时间分布分析（按小时）"
echo "----------------------------------------"
awk -F'|' '{
    # 提取时间戳并格式化为小时
    split($1, dt, "T");
    date_part = dt[1];
    split(dt[2], tm, ":");
    hour = tm[1];
    time_key = date_part " " hour ":00";
    
    # 统计该小时的失败次数
    count[time_key]++;
}
END {
    for (t in count) {
        print t, count[t];
    }
}' "$DEAD_LETTER_FILE" | sort | \
    awk '{printf "%-20s : %d 次\n", $1" "$2, $3}'
echo ""

# 4. 最严重的失败时刻（失败 ID 最多的批次）
echo "## 4. 最严重的失败批次（Top 5）"
echo "----------------------------------------"
awk -F'|' '{
    # 提取时间和 ID 列表
    time = $1;
    ids = $2;
    reason = $3;
    
    # 计算 ID 数量
    gsub(/[^0-9 ]/, "", ids);
    split(ids, id_arr, " ");
    count = 0;
    for (i in id_arr) {
        if (id_arr[i] != "") count++;
    }
    
    # 保存
    print count, time, reason;
}' "$DEAD_LETTER_FILE" | sort -rn | head -5 | \
    awk '{
        printf "%s - %d 条 - %s\n", $2, $1, substr($0, index($0,$3));
    }'
echo ""

# 5. 提取失败的 ID（去重）
echo "## 5. 失败 ID 信息"
echo "----------------------------------------"
TMP_IDS="/tmp/failed_ids_${TASK}.txt"
grep -o '\[[^]]*\]' "$DEAD_LETTER_FILE" | \
    tr -d '[]' | tr ' ' '\n' | \
    grep -E '^[0-9]+$' | sort -n | uniq > "$TMP_IDS"

UNIQUE_IDS=$(wc -l < "$TMP_IDS")
echo "唯一失败 ID 数: $UNIQUE_IDS"
echo "ID 范围: $(head -1 "$TMP_IDS") ~ $(tail -1 "$TMP_IDS")"
echo "失败 ID 已保存到: $TMP_IDS"
echo ""

# 6. 最近的失败
echo "## 6. 最近 5 次失败"
echo "----------------------------------------"
tail -5 "$DEAD_LETTER_FILE" | while IFS='|' read -r time ids reason; do
    # 计算 ID 数量
    id_count=$(echo "$ids" | grep -o '[0-9]\+' | wc -l)
    echo "时间: $time"
    echo "数量: $id_count 条"
    echo "原因: $reason"
    echo "---"
done
echo ""

# 7. 建议
echo "## 7. 问题诊断与建议"
echo "----------------------------------------"

# 检查 413 错误
if grep -q "413\|Request Entity Too Large" "$DEAD_LETTER_FILE"; then
    echo "🔴 发现 ES 413 错误（请求体过大）"
    echo "   原因: bulk.size 配置过大，或 pending overflow 导致单次写入过多"
    echo "   建议: "
    echo "   1. 减小 bulk.size 到 200-300"
    echo "   2. 检查 ES http.max_content_length 配置"
    echo "   3. 确保已应用分批 flush 补丁"
    echo ""
fi

# 检查 broken pipe
if grep -q "broken pipe" "$DEAD_LETTER_FILE"; then
    echo "🔴 发现 Broken Pipe 错误（连接断开）"
    echo "   原因: ES 处理超时或网络不稳定"
    echo "   建议: "
    echo "   1. 减小 bulk.size"
    echo "   2. 增加 esBulkTimeoutMs 到 120000"
    echo "   3. 检查 ES 集群健康状态"
    echo ""
fi

# 检查版本冲突
if grep -q "version_conflict" "$DEAD_LETTER_FILE"; then
    echo "⚠️  发现版本冲突"
    echo "   原因: 并发写入或重复处理"
    echo "   说明: 系统会自动重算，如果重算失败才会记录死信"
    echo ""
fi

# 检查 SQL 失败
if grep -q "query.*failed\|sql.*failed" "$DEAD_LETTER_FILE"; then
    echo "🔴 发现 SQL 查询失败"
    echo "   原因: 数据库连接问题或 SQL 性能问题"
    echo "   建议: "
    echo "   1. 检查数据库连接"
    echo "   2. 优化主 SQL 性能"
    echo "   3. 添加必要的索引"
    echo ""
fi

# 8. 恢复建议
echo "## 8. 数据恢复建议"
echo "----------------------------------------"
echo "方法 1: 重放死信（推荐）"
echo "  ./bin/binlog-es-go --mode=replay-deadletters --task=$TASK"
echo ""
echo "方法 2: 按 ID 列表恢复"
echo "  # 使用生成的 ID 文件"
echo "  cat $TMP_IDS | head -1000 | tr '\\n' ',' | sed 's/,\$//' | xargs -I {} \\"
echo "    ./bin/binlog-es-go --mode=bootstrap --task=$TASK \\"
echo "    --bootstrap.sql-where=\"s.AutoID IN ({})\""
echo ""
echo "方法 3: 按时间范围恢复（如果知道失败时间段）"
echo "  # 从死信文件获取时间范围"
echo "  START_TIME=\$(head -1 $DEAD_LETTER_FILE | cut -d'|' -f1)"
echo "  END_TIME=\$(tail -1 $DEAD_LETTER_FILE | cut -d'|' -f1)"
echo "  ./bin/binlog-es-go --mode=bootstrap --task=$TASK \\"
echo "    --bootstrap.sql-where=\"s.update_date >= '\$START_TIME' AND s.update_date < '\$END_TIME'\""
echo ""

echo "========================================="
echo "  分析完成"
echo "========================================="
