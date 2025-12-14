package tracking

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
)

// FailureRecord 记录单次失败的详细信息
type FailureRecord struct {
	Timestamp     string   `json:"timestamp"`      // 失败时间
	IDs           []int64  `json:"ids"`            // 失败的 ID 列表
	IDCount       int      `json:"id_count"`       // ID 数量
	FailureType   string   `json:"failure_type"`   // 失败类型：sql_query/es_bulk/es_conflict/network
	Reason        string   `json:"reason"`         // 失败原因（错误信息）
	Stage         string   `json:"stage"`          // 失败阶段：realtime/bootstrap/replay
	BinlogFile    string   `json:"binlog_file"`    // binlog 文件名（realtime 模式）
	BinlogPos     uint32   `json:"binlog_pos"`     // binlog 位置
	GTID          string   `json:"gtid"`           // GTID 位点
	EventType     string   `json:"event_type"`     // binlog 事件类型：insert/update/delete/related
	SourceTable   string   `json:"source_table"`   // 来源表
	RetryAttempts int      `json:"retry_attempts"` // 重试次数
	Context       string   `json:"context"`        // 额外上下文信息
}

// FailureTracker 失败追踪器
type FailureTracker struct {
	task      string
	log       *zap.Logger
	mu        sync.Mutex
	file      *os.File
	jsonFile  *os.File // JSON 格式文件，方便程序解析
}

// NewFailureTracker 创建失败追踪器
func NewFailureTracker(task string, log *zap.Logger) (*FailureTracker, error) {
	dir := filepath.Join("logs", "failure-tracking")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	// 文本格式文件（人类可读）
	textPath := filepath.Join(dir, fmt.Sprintf("%s.log", task))
	textFile, err := os.OpenFile(textPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	// JSON 格式文件（程序可解析）
	jsonPath := filepath.Join(dir, fmt.Sprintf("%s.jsonl", task))
	jsonFile, err := os.OpenFile(jsonPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		textFile.Close()
		return nil, err
	}

	return &FailureTracker{
		task:     task,
		log:      log,
		file:     textFile,
		jsonFile: jsonFile,
	}, nil
}

// RecordFailure 记录一次失败
func (ft *FailureTracker) RecordFailure(record FailureRecord) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	record.Timestamp = time.Now().Format(time.RFC3339)
	record.IDCount = len(record.IDs)

	// 写入 JSON 格式（一行一个 JSON 对象）
	jsonData, err := json.Marshal(record)
	if err != nil {
		ft.log.Warn("marshal failure record failed", zap.Error(err))
		return
	}
	ft.jsonFile.Write(jsonData)
	ft.jsonFile.Write([]byte("\n"))

	// 写入文本格式（人类可读）
	// 格式：时间|类型|阶段|ID数量|原因|上下文
	textLine := fmt.Sprintf("%s|%s|%s|%d IDs|%s|binlog:%s@%d|gtid:%s|%s\n",
		record.Timestamp,
		record.FailureType,
		record.Stage,
		record.IDCount,
		truncateString(record.Reason, 100),
		record.BinlogFile,
		record.BinlogPos,
		truncateString(record.GTID, 50),
		record.Context,
	)
	ft.file.WriteString(textLine)

	// 如果 ID 数量不多，记录详细 ID 列表
	if len(record.IDs) <= 100 {
		idLine := fmt.Sprintf("  └─ IDs: %v\n", record.IDs)
		ft.file.WriteString(idLine)
	} else {
		// ID 太多，只记录样例
		sample := record.IDs[:10]
		idLine := fmt.Sprintf("  └─ IDs (sample): %v ... (total: %d)\n", sample, len(record.IDs))
		ft.file.WriteString(idLine)
	}

	ft.log.Warn("记录失败追踪",
		zap.String("type", record.FailureType),
		zap.Int("ids", len(record.IDs)),
		zap.String("reason", truncateString(record.Reason, 100)),
	)
}

// Close 关闭追踪器
func (ft *FailureTracker) Close() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	if ft.file != nil {
		ft.file.Close()
	}
	if ft.jsonFile != nil {
		ft.jsonFile.Close()
	}
	return nil
}

// truncateString 截断字符串
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// AnalyzeFailures 分析失败记录（工具函数）
func AnalyzeFailures(task string) (map[string]int, error) {
	jsonPath := filepath.Join("logs", "failure-tracking", fmt.Sprintf("%s.jsonl", task))
	file, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stats := make(map[string]int)
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var record FailureRecord
		if err := decoder.Decode(&record); err != nil {
			continue
		}
		stats[record.FailureType] += record.IDCount
	}

	return stats, nil
}
