package bootstrap

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	"github.com/cookchen233/binlog-es-go/pkg/db"
	espkg "github.com/cookchen233/binlog-es-go/pkg/es"
	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/mapper"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/sink"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/transform"
)

// Runner executes full bootstrap sync for a single task.
type Runner struct {
	log   *zap.Logger
	cfg   cfgpkg.Config
	task  cfgpkg.SyncTask
	mysql *db.MySQL
	es    *espkg.Writer
}

// RunWithIDs upserts a provided list of ids for the current task in batches.
func (r *Runner) RunWithIDs(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		r.log.Info("RunWithIDs: empty ids, nothing to do")
		return nil
	}
	mainSQL := r.task.Mapping.SQL
	index := r.task.Mapping.Index
	sharding := r.task.Mapping.Sharding
	// batch in chunks (configurable): prefer task.Bulk.Size > cfg.Bootstrap.BulkSize > cfg.Bootstrap.RunBatchSize > 1000
	batch := 1000
	if r.task.Bulk.Size > 0 {
		batch = r.task.Bulk.Size
	} else if r.cfg.Bootstrap.BulkSize > 0 {
		batch = r.cfg.Bootstrap.BulkSize
	} else if r.cfg.Bootstrap.RunBatchSize > 0 {
		batch = r.cfg.Bootstrap.RunBatchSize
	}
	// 创建 BulkWriter（关闭熔断）
	bw := sink.NewBulkWriter(r.es, r.log, sink.Options{Retry: sink.RetryConfig{MaxAttempts: r.task.Retry.MaxAttempts, BackoffMs: r.task.Retry.BackoffMs}, CircuitEnabled: false, MaxBackoff: 30 * time.Second})
	for offset := 0; offset < len(ids); offset += batch {
		end := offset + batch
		if end > len(ids) {
			end = len(ids)
		}
		sub := ids[offset:end]
		groups := groupKeysByShard(sub, sharding)
		for shard, shardKeys := range groups {
			if len(shardKeys) == 0 {
				continue
			}
			qSQL := renderSQLTemplate(mainSQL, shard, sharding)
			var rows []map[string]interface{}
			rows, _, qErr := mapper.Do(ctx, 30*time.Second, mapper.RetryConfig{MaxAttempts: r.task.Retry.MaxAttempts, BackoffMs: r.task.Retry.BackoffMs}, r.log, func(qctx context.Context) ([]map[string]interface{}, error) {
				res, err := r.mysql.QueryMapping(qctx, qSQL, shardKeys)
				if err != nil {
					r.log.Warn("query mapping retry", zap.Error(err), zap.Int("count", len(shardKeys)))
					metrics.RetryTotal.WithLabelValues("sql").Inc()
				}
				return res, err
			})
			if qErr != nil {
				r.log.Error("RunWithIDs: query failed", zap.Error(qErr), zap.Int("count", len(shardKeys)))
				writeDeadLetters(r.log, r.task.Destination, shardKeys, fmt.Sprintf("query mapping failed: %v", qErr))
				continue
			}
			if len(rows) == 0 {
				r.log.Info("RunWithIDs: no docs for batch", zap.Int("count", len(shardKeys)))
				continue
			}
			docs := make([]map[string]interface{}, 0, len(rows))
			for _, row := range rows {
				doc := transform.NormalizeBytesToString(row)
				if len(r.task.Transforms.JSONDecodeFields) > 0 {
					transform.JSONDecodeFields(doc, r.task.Transforms.JSONDecodeFields)
				}
				if len(r.task.Transforms.SplitFields) > 0 {
					for _, rule := range r.task.Transforms.SplitFields {
						if rule.Field == "" {
							continue
						}
						transform.SplitStringField(doc, rule.Field, rule.Sep, rule.Trim)
					}
				}
				docs = append(docs, doc)
			}
			upserted, bErr := bw.Upsert(ctx, 60*time.Second, index, r.task.Mapping.ID, docs)
			if bErr != nil {
				if be, ok := bErr.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
					_, err := bw.HandleConflictIDs(
						ctx,
						be.ConflictedIDs,
						30*time.Second,
						60*time.Second,
						index,
						r.task.Mapping.ID,
						qSQL,
						mapper.RetryConfig{MaxAttempts: r.task.Retry.MaxAttempts, BackoffMs: r.task.Retry.BackoffMs},
						r.mysql.QueryMapping,
						r.task.Transforms.SplitFields,
						r.task.Transforms.JSONDecodeFields,
						"RunWithIDs",
					)
					if err != nil {
						continue
					}
					continue
				}
				r.log.Error("RunWithIDs: bulk failed", zap.Error(bErr), zap.Int("docs", len(docs)))
				writeDeadLetters(r.log, r.task.Destination, shardKeys, fmt.Sprintf("es bulk failed: %v", bErr))
				continue
			}
			r.log.Info("RunWithIDs: batch upserted", zap.Int("docs", len(docs)), zap.Int("upserted", upserted), zap.Int("offset", offset), zap.Int("shard", shard))
		}
	}
	return nil
}

// writeDeadLetters appends failed ids with reason to dead-letters/<task>.log
func writeDeadLetters(log *zap.Logger, task string, ids []int64, reason string) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn("write dead-letters panic", zap.Any("recover", r))
		}
	}()
	if task == "" || len(ids) == 0 {
		return
	}
	dir := filepath.Join("logs", "dead-letters")
	_ = os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, task+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Warn("open dead-letters failed", zap.Error(err), zap.String("path", path))
		return
	}
	defer f.Close()
	ts := time.Now().Format(time.RFC3339)
	// 写入简单行：时间|ids|reason
	line := fmt.Sprintf("%s|%v|%s\n", ts, ids, reason)
	if _, err := f.WriteString(line); err != nil {
		log.Warn("write dead-letters failed", zap.Error(err), zap.String("path", path))
	}
	// 指标：死信数量（按ID数量累加）
	if len(ids) > 0 {
		metrics.DeadLettersTotal.Add(float64(len(ids)))
	}
}

// ResolveAutoIDRange queries MIN/MAX of the main table's key column to provide defaults.
func (r *Runner) ResolveAutoIDRange(ctx context.Context) (int64, int64, error) {
	mainTable := getMainTableName(r.task.Mapping.SQL)
	keyCol := "id"
	if arr, ok := r.task.MappingTable[strings.ToLower(mainTable)]; ok && len(arr) > 0 {
		keyCol = arr[0]
	}
	return r.mysql.GetMinMax(ctx, mainTable, keyCol)
}

// NewRunner builds a bootstrap runner.
func NewRunner(log *zap.Logger, cfg cfgpkg.Config, task cfgpkg.SyncTask) (*Runner, error) {
	mysql, err := db.NewMySQL(cfg.DataSource.DSN)
	if err != nil {
		return nil, err
	}
	// 注入日志与SQL调试开关
	mysql.SetLogger(log)
	mysql.SetDebug(cfg.Debug.SQL, cfg.Debug.SQLParams)
	esw, err := espkg.NewWithTLS(cfg.ES.Addresses, cfg.ES.Username, cfg.ES.Password, espkg.TLSConfig{
		InsecureSkipVerify: cfg.ES.TLS.InsecureSkipVerify,
		CAFile:             cfg.ES.TLS.CAFile,
		CertFile:           cfg.ES.TLS.CertFile,
		KeyFile:            cfg.ES.TLS.KeyFile,
		ServerName:         cfg.ES.TLS.ServerName,
	})
	if err != nil {
		return nil, err
	}
	if cfg.ES.Refresh != "" {
		esw.SetRefresh(cfg.ES.Refresh)
		log.Info("es refresh policy enabled", zap.String("refresh", cfg.ES.Refresh), zap.Strings("addresses", cfg.ES.Addresses))
	} else {
		log.Info("es refresh policy not set (using index refresh interval)", zap.Strings("addresses", cfg.ES.Addresses))
	}
	// Fail fast if index was not initialized (prod often deploys es-init separately)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := esw.IndexExists(ctx, task.Mapping.Index)
	if err != nil {
		return nil, fmt.Errorf("es index exists check failed: index=%s err=%w", task.Mapping.Index, err)
	}
	if !exists {
		return nil, fmt.Errorf("es index does not exist: %s (initialize index/mapping before running bootstrap)", task.Mapping.Index)
	}
	return &Runner{log: log, cfg: cfg, task: task, mysql: mysql, es: esw}, nil
}

// injectWhere adds additional AND condition into the first WHERE clause.
func injectWhere(sql string, where string) string {
	if strings.TrimSpace(where) == "" {
		return sql
	}
	// 使用不区分大小写的正则，将首个 WHERE 后插入附加条件
	re := regexp.MustCompile(`(?i)where\s+`)
	loc := re.FindStringIndex(sql)
	if loc == nil {
		// 未找到 WHERE，直接附加 WHERE 子句
		return sql + " WHERE " + where
	}
	// 在首个 WHERE 后插入条件和 AND
	return sql[:loc[1]] + where + " AND " + sql[loc[1]:]
}

// getMainTableName tries to parse the main table from the mapping SQL's FROM clause.
func getMainTableName(sql string) string {
	re := regexp.MustCompile("(?i)from\\s+`?([\\w\\.]+)`?\\s*")
	m := re.FindStringSubmatch(sql)
	if len(m) >= 2 {
		t := m[1]
		if strings.Contains(t, ".") {
			parts := strings.SplitN(t, ".", 2)
			t = parts[1]
		}
		return strings.ToLower(t)
	}
	return ""
}

func shardSuffix(shard int, width int) string {
	if width <= 0 {
		width = 2
	}
	s := strconv.Itoa(shard)
	if len(s) >= width {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

func renderSQLTemplate(sql string, shard int, sharding *cfgpkg.ShardingConfig) string {
	if sql == "" || sharding == nil || len(sharding.Vars) == 0 {
		return sql
	}
	suffix := shardSuffix(shard, sharding.SuffixWidth)
	out := sql
	for v, base := range sharding.Vars {
		if strings.TrimSpace(v) == "" || strings.TrimSpace(base) == "" {
			continue
		}
		out = strings.ReplaceAll(out, "{{"+v+"}}", base+"_"+suffix)
	}
	return out
}

func shardForKey(key int64, sharding *cfgpkg.ShardingConfig) int {
	if sharding == nil || sharding.Shards <= 0 {
		return 0
	}
	strategy := strings.ToLower(strings.TrimSpace(sharding.Strategy))
	if strategy == "" {
		strategy = "crc32_ieee_uint32"
	}
	switch strategy {
	case "crc32_ieee_uint32":
		s := strconv.FormatInt(key, 10)
		u := crc32.ChecksumIEEE([]byte(s))
		return int(int64(u) % int64(sharding.Shards))
	case "crc32", "crc32_ieee_signed_abs":
		s := strconv.FormatInt(key, 10)
		u := crc32.ChecksumIEEE([]byte(s))
		v := int64(int32(u))
		if v < 0 {
			v = -v
		}
		return int(v % int64(sharding.Shards))
	default:
		if key < 0 {
			key = -key
		}
		return int(key % int64(sharding.Shards))
	}
}

func groupKeysByShard(keys []int64, sharding *cfgpkg.ShardingConfig) map[int][]int64 {
	groups := make(map[int][]int64)
	if len(keys) == 0 {
		return groups
	}
	if sharding == nil || sharding.Shards <= 0 {
		groups[0] = keys
		return groups
	}
	for _, k := range keys {
		sh := shardForKey(k, sharding)
		groups[sh] = append(groups[sh], k)
	}
	return groups
}

func detectTemplateVarFromSQL(sql string) string {
	re := regexp.MustCompile(`(?i)from\s+\{\{\s*([a-zA-Z0-9_]+)\s*\}\}`)
	m := re.FindStringSubmatch(sql)
	if len(m) == 2 {
		return m[1]
	}
	return ""
}

func (r *Runner) RunFullScan(ctx context.Context, pageSize int, workers int, extraWhere string) error {
	if pageSize <= 0 {
		pageSize = 1000
	}
	if workers <= 0 {
		workers = 1
	}
	mainSQL := injectWhere(r.task.Mapping.SQL, extraWhere)
	logicalMain := strings.ToLower(strings.TrimSpace(r.task.Mapping.MainTable))
	if logicalMain == "" {
		logicalMain = getMainTableName(mainSQL)
	}
	keyCol := "id"
	if arr, ok := r.task.MappingTable[strings.ToLower(logicalMain)]; ok && len(arr) > 0 {
		keyCol = arr[0]
	}
	sharding := r.task.Mapping.Sharding

	baseTable := logicalMain
	if sharding != nil && len(sharding.Vars) > 0 {
		if v := detectTemplateVarFromSQL(mainSQL); v != "" {
			if b, ok := sharding.Vars[v]; ok && strings.TrimSpace(b) != "" {
				baseTable = strings.ToLower(strings.TrimSpace(b))
			}
		}
	}

	tables := []struct {
		name  string
		shard int
	}{{name: baseTable, shard: 0}}
	if sharding != nil && sharding.Shards > 0 {
		tables = make([]struct {
			name  string
			shard int
		}, 0, sharding.Shards)
		for i := 0; i < sharding.Shards; i++ {
			suffix := shardSuffix(i, sharding.SuffixWidth)
			tables = append(tables, struct {
				name  string
				shard int
			}{name: baseTable + "_" + suffix, shard: i})
		}
	}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	for _, t := range tables {
		wg.Add(1)
		sem <- struct{}{}
		table := t
		go func() {
			defer func() {
				<-sem
				wg.Done()
			}()
			last := int64(0)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				ids, err := r.mysql.GetIDsAfter(ctx, table.name, keyCol, last, pageSize)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				if len(ids) == 0 {
					return
				}
				last = ids[len(ids)-1]
				qSQL := renderSQLTemplate(mainSQL, table.shard, sharding)
				subRunner := *r
				subRunner.task.Mapping.SQL = qSQL
				if err := subRunner.RunWithIDs(ctx, ids); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// Run executes from minAutoID to maxAutoID (exclusive) stepping by partSize, with worker parallelism.
func (r *Runner) Run(ctx context.Context, minAutoID, maxAutoID int64, partSize int, workers int, extraWhere string) error {
	if maxAutoID <= 0 || maxAutoID <= minAutoID {
		return fmt.Errorf("invalid range: min=%d max=%d", minAutoID, maxAutoID)
	}
	mainSQL := injectWhere(r.task.Mapping.SQL, extraWhere)
	index := r.task.Mapping.Index
	mainTable := getMainTableName(mainSQL)
	keyCol := "id"
	if arr, ok := r.task.MappingTable[strings.ToLower(mainTable)]; ok && len(arr) > 0 {
		keyCol = arr[0]
	}

	type part struct{ start, end int64 }
	parts := make(chan part, workers*2)
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	var processedParts int64

	// 优化：如果 extraWhere 里是精确主键匹配（不依赖别名），收缩扫描范围
	if strings.TrimSpace(extraWhere) != "" && keyCol != "" {
		// 构造匹配当前任务主键列名的正则，支持可选别名，如 "s.<keyCol> = 123" 或 "<keyCol>=123"
		safeKey := regexp.QuoteMeta(keyCol)
		reExact := regexp.MustCompile(`(?i)\b(?:\w+\.)?` + safeKey + `\s*=\s*([0-9]+)`) // e.g. s.AutoID = 123 或 AutoID=123
		if m := reExact.FindStringSubmatch(extraWhere); len(m) == 2 {
			var v int64
			fmt.Sscanf(m[1], "%d", &v)
			if v > 0 {
				r.log.Info("detected exact key match in where, narrowing scan range", zap.String("key", keyCol), zap.Int64("value", v))
				minAutoID, maxAutoID = v, v+1
			}
		}
	}

	totalParts := (maxAutoID - minAutoID + int64(partSize) - 1) / int64(partSize)
	r.log.Info("bootstrap scan plan",
		zap.Int64("min_auto_id", minAutoID),
		zap.Int64("max_auto_id", maxAutoID),
		zap.Int("partition_size", partSize),
		zap.Int64("total_partitions", totalParts),
		zap.String("index", index),
	)

	// workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					r.log.Error("bootstrap worker panic", zap.Any("recover", rec), zap.ByteString("stack", debug.Stack()))
				}
				wg.Done()
			}()
			// 每个 worker 自建一个 BulkWriter（关闭熔断，避免状态竞争）
			bw := sink.NewBulkWriter(r.es, r.log, sink.Options{Retry: sink.RetryConfig{MaxAttempts: r.task.Retry.MaxAttempts, BackoffMs: r.task.Retry.BackoffMs}, CircuitEnabled: false, MaxBackoff: 30 * time.Second})
			for p := range parts {
				select {
				case <-ctx.Done():
					return
				default:
				}
				// fetch IDs of main table in partition
				ids, err := r.mysql.GetIDs(ctx, mainTable, keyCol, p.start, p.end, partSize)
				if err != nil {
					r.log.Error("get auto ids failed", zap.Error(err), zap.Int64("start", p.start), zap.Int64("end", p.end))
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				if len(ids) == 0 {
					// 空分片，计数并继续
					mu.Lock()
					processedParts++
					mu.Unlock()
					continue
				}
				// 调试：打印SQL执行概况
				if r.cfg.Debug.SQL {
					sampleN := len(ids)
					if sampleN > 5 {
						sampleN = 5
					}
					r.log.Debug("execute mapping sql", zap.String("table", mainTable), zap.String("key", keyCol), zap.Int("ids", len(ids)), zap.Any("ids.sample", ids[:sampleN]))
				}
				// query mapping rows (with retry via mapper.Do)
				var rows []map[string]interface{}
				rows, _, qErr := mapper.Do(ctx, 30*time.Second, mapper.RetryConfig{MaxAttempts: r.task.Retry.MaxAttempts, BackoffMs: r.task.Retry.BackoffMs}, r.log, func(qctx context.Context) ([]map[string]interface{}, error) {
					res, err := r.mysql.QueryMapping(qctx, mainSQL, ids)
					if err != nil {
						r.log.Warn("query mapping retry", zap.Error(err), zap.Int("count", len(ids)))
						metrics.RetryTotal.WithLabelValues("sql").Inc()
					}
					return res, err
				})
				if qErr != nil {
					r.log.Error("query mapping failed", zap.Error(qErr), zap.Int("count", len(ids)))
					// 记录死信
					writeDeadLetters(r.log, r.task.Destination, ids, fmt.Sprintf("query mapping failed: %v", qErr))
					mu.Lock()
					if firstErr == nil {
						firstErr = qErr
					}
					mu.Unlock()
					continue
				}
				if len(rows) == 0 {
					mu.Lock()
					processedParts++
					mu.Unlock()
					continue
				}
				// normalize docs for ES
				docs := make([]map[string]interface{}, 0, len(rows))
				for _, row := range rows {
					doc := transform.NormalizeBytesToString(row)
					if len(r.task.Transforms.JSONDecodeFields) > 0 {
						transform.JSONDecodeFields(doc, r.task.Transforms.JSONDecodeFields)
					}
					// 应用 transforms.splitFields 规则
					if len(r.task.Transforms.SplitFields) > 0 {
						for _, rule := range r.task.Transforms.SplitFields {
							if rule.Field == "" {
								continue
							}
							transform.SplitStringField(doc, rule.Field, rule.Sep, rule.Trim)
						}
					}
					docs = append(docs, doc)
				}
				// 调试日志：通用预览前几条 doc 的字段
				if len(docs) > 0 && r.cfg.Debug.SQL {
					preview := transform.PreviewTopN(docs[0], 5)
					r.log.Debug("doc preview before upsert", zap.Any("preview", preview))
				}
				// upsert via BulkWriter（内部已带重试与指标）
				upserted, bErr := bw.Upsert(ctx, 60*time.Second, index, r.task.Mapping.ID, docs)
				if bErr != nil {
					// 冲突回退：仅针对版本冲突ID重算并重试一次
					if be, ok := bErr.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
						_, err := bw.HandleConflictIDs(
							ctx,
							be.ConflictedIDs,
							30*time.Second,
							60*time.Second,
							index,
							r.task.Mapping.ID,
							mainSQL,
							mapper.RetryConfig{MaxAttempts: r.task.Retry.MaxAttempts, BackoffMs: r.task.Retry.BackoffMs},
							r.mysql.QueryMapping,
							r.task.Transforms.SplitFields,
							r.task.Transforms.JSONDecodeFields,
							"bootstrap",
						)
						if err != nil {
							// HandleConflictIDs内部已记录死信，这里不再处理
						}
						// 冲突分支处理完毕，继续后续循环
						continue
					}
					r.log.Error("es bulk upsert failed", zap.Error(bErr), zap.Int("docs", len(docs)))
					// 记录死信（记录本批 ids）
					writeDeadLetters(r.log, r.task.Destination, ids, fmt.Sprintf("es bulk failed: %v", bErr))
					mu.Lock()
					if firstErr == nil {
						firstErr = bErr
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				processedParts++
				cur := processedParts
				tp := totalParts
				mu.Unlock()
				r.log.Info("partition synced", zap.Int("docs", len(docs)), zap.Int("upserted", upserted), zap.Int64("start", p.start), zap.Int64("end", p.end), zap.Int64("progress", cur), zap.Int64("total", tp))
			}
		}()
	}

	// feed partitions
	for cur := minAutoID; cur < maxAutoID; cur += int64(partSize) {
		end := cur + int64(partSize)
		if end > maxAutoID {
			end = maxAutoID
		}
		select {
		case <-ctx.Done():
			close(parts)
			wg.Wait()
			return ctx.Err()
		case parts <- part{start: cur, end: end}:
		}
	}
	close(parts)
	wg.Wait()
	return firstErr
}
