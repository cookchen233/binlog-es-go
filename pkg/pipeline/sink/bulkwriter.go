package sink

import (
	"context"
	"fmt"
	"time"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	espkg "github.com/cookchen233/binlog-es-go/pkg/es"
	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/mapper"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/transform"
	"github.com/cookchen233/binlog-es-go/pkg/util"
	"go.uber.org/zap"
)

// RetryConfig mirrors mapper retry.
type RetryConfig struct {
	MaxAttempts int
	BackoffMs   []int
}

// Options configures BulkWriter behavior.
type Options struct {
	Retry          RetryConfig
	CircuitEnabled bool
	MaxBackoff     time.Duration // max backoff used when circuit opens
}

// BulkWriter wraps ES writer with retry, metrics and optional circuit breaker.
type BulkWriter struct {
	es   *espkg.Writer
	log  *zap.Logger
	opts Options

	// circuit state
	consecFail int
	cbUntil    time.Time
}

func NewBulkWriter(es *espkg.Writer, log *zap.Logger, opts Options) *BulkWriter {
	bw := &BulkWriter{es: es, log: log, opts: opts}
	if bw.opts.MaxBackoff <= 0 {
		bw.opts.MaxBackoff = 30 * time.Second
	}
	return bw
}

// waitIfCircuitOpen sleeps until circuit closes or ctx done.
func (b *BulkWriter) waitIfCircuitOpen(ctx context.Context) error {
	if !b.opts.CircuitEnabled {
		return nil
	}
	if time.Now().Before(b.cbUntil) {
		sleep := time.Until(b.cbUntil)
		b.log.Warn("es circuit open, sleep", zap.Duration("sleep", sleep))
		select {
		case <-time.After(sleep):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *BulkWriter) onSuccess(kind string, n int, elapsed time.Duration) {
	// reset circuit
	b.consecFail = 0
	b.cbUntil = time.Time{}
	// metrics
	if kind == "upsert" {
		if n > 0 {
			metrics.UpsertSuccessTotal.Add(float64(n))
		}
	} else if kind == "delete" {
		if n > 0 {
			metrics.DeleteSuccessTotal.WithLabelValues("delete").Add(float64(n))
		}
	}
	metrics.ESBulkLatencySeconds.Observe(elapsed.Seconds())
}

func (b *BulkWriter) onFailure(msg string, err error) {
	// backoff & open circuit
	if !b.opts.CircuitEnabled {
		return
	}
	b.consecFail++
	backoff := time.Second << (b.consecFail - 1)
	if backoff > b.opts.MaxBackoff {
		backoff = b.opts.MaxBackoff
	}
	b.cbUntil = time.Now().Add(backoff)
	b.log.Warn(msg, zap.Int("consecutive_fail", b.consecFail), zap.Duration("backoff", backoff), zap.Error(err))
}

// Upsert performs bulk upsert with retry, metrics, and optional circuit breaker.
func (b *BulkWriter) Upsert(ctx context.Context, timeout time.Duration, index, idField string, docs []map[string]interface{}) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	if err := b.waitIfCircuitOpen(ctx); err != nil {
		return 0, err
	}
	var affected int
	var elapsed time.Duration
	err := util.Retry(b.opts.Retry.MaxAttempts, b.opts.Retry.BackoffMs, func() error {
		ctx2, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		start := time.Now()
		n, e := b.es.UpsertBulk(ctx2, index, idField, docs)
		elapsed = time.Since(start)
		affected = n
		if e != nil {
			// 特殊处理：若为版本冲突，立即交由上层进行冲突ID重算，不继续在此层重试
			if be, ok := e.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
				b.log.Warn("检测到版本冲突(Upsert), 交由上层重算", zap.Any("conflicted", be.ConflictedIDs))
				return e
			}
			metrics.RetryTotal.WithLabelValues("es").Inc()
			b.log.Warn("es bulk upsert retry", zap.Error(e), zap.Int("docs", len(docs)))
		}
		return e
	}, nil)
	if err != nil {
		b.onFailure("es bulk failed, open circuit", err)
		return 0, err
	}
	b.onSuccess("upsert", affected, elapsed)
	return affected, nil
}

// Delete performs bulk delete with retry, metrics, and optional circuit breaker.
func (b *BulkWriter) Delete(ctx context.Context, timeout time.Duration, index string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if err := b.waitIfCircuitOpen(ctx); err != nil {
		return 0, err
	}
	var affected int
	var elapsed time.Duration
	err := util.Retry(b.opts.Retry.MaxAttempts, b.opts.Retry.BackoffMs, func() error {
		ctx2, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		start := time.Now()
		n, e := b.es.DeleteBulk(ctx2, index, ids)
		elapsed = time.Since(start)
		affected = n
		if e != nil {
			if be, ok := e.(*espkg.BulkError); ok && len(be.ConflictedIDs) > 0 {
				b.log.Warn("检测到版本冲突(Delete), 交由上层重算", zap.Any("conflicted", be.ConflictedIDs))
				return e
			}
			metrics.RetryTotal.WithLabelValues("es").Inc()
			b.log.Warn("es bulk delete retry", zap.Error(e), zap.Int("ids", len(ids)))
		}
		return e
	}, nil)
	if err != nil {
		b.onFailure("es delete failed, open circuit", err)
		return 0, err
	}
	b.onSuccess("delete", affected, elapsed)
	return affected, nil
}

// HandleConflictIDs 处理版本冲突后的ID重算和重试写入
// 参数:
//   - ctx: 上下文
//   - conflictedIDs: 冲突的ID字符串列表
//   - qTimeout: 查询超时时间
//   - esTimeout: ES写入超时时间
//   - index: ES索引名
//   - idField: ES文档ID字段名
//   - mainSQL: 查询SQL
//   - retryConfig: 重试配置
//   - queryFn: 查询函数，接收context、SQL和ID列表，返回查询结果
//   - transformRules: 数据转换规则
//   - jsonDecodeFields: JSON解码字段列表
//   - logContext: 日志上下文信息
//
// 返回值:
//   - 重试成功的文档数量
//   - 错误信息

func (b *BulkWriter) HandleConflictIDs(
	ctx context.Context,
	conflictedIDs []string,
	qTimeout time.Duration,
	esTimeout time.Duration,
	index string,
	idField string,
	mainSQL string,
	retryCfg mapper.RetryConfig,
	queryFn func(context.Context, string, []int64) ([]map[string]interface{}, error),
	transformRules []cfgpkg.SplitFieldRule,
	jsonDecodeFields []string,
	logContext string,
) (int, error) {
	if len(conflictedIDs) == 0 {
		return 0, nil
	}

	// 将字符串ID转换为int64
	ids2 := make([]int64, 0, len(conflictedIDs))
	for _, s := range conflictedIDs {
		var v int64
		fmt.Sscanf(s, "%d", &v)
		if v > 0 {
			ids2 = append(ids2, v)
		}
	}

	b.log.Warn("检测到版本冲突, 启动冲突ID重算",
		zap.String("context", logContext),
		zap.Any("conflicted", conflictedIDs))

	// 重新查询冲突ID对应的数据
	var rows2 []map[string]interface{}
	rows2, _, _ = mapper.Do(ctx, qTimeout, retryCfg, b.log, func(qctx context.Context) ([]map[string]interface{}, error) {
		return queryFn(qctx, mainSQL, ids2)
	})

	if len(rows2) == 0 {
		b.log.Warn("冲突ID重算无返回行",
			zap.String("context", logContext),
			zap.Int("ids", len(ids2)))
		// 记录死信 - 与原代码保持一致
		b.log.Error("记录死信", zap.String("context", logContext), zap.Any("ids", ids2), zap.String("reason", "recompute returned no rows for conflicted ids"))
		return 0, nil
	}

	// 转换数据格式并应用转换规则
	docs2 := make([]map[string]interface{}, 0, len(rows2))
	for _, row := range rows2 {
		doc := transform.NormalizeBytesToString(row)
		if len(jsonDecodeFields) > 0 {
			transform.JSONDecodeFields(doc, jsonDecodeFields)
		}
		if len(transformRules) > 0 {
			for _, rule := range transformRules {
				if rule.Field == "" {
					continue
				}
				transform.SplitStringField(doc, rule.Field, rule.Sep, rule.Trim)
			}
		}
		docs2 = append(docs2, doc)
	}

	// 重试写入ES
	affected, err := b.Upsert(ctx, esTimeout, index, idField, docs2)
	if err != nil {
		b.log.Error("冲突ID重算后仍写入失败",
			zap.String("context", logContext),
			zap.Error(err),
			zap.Int("docs", len(docs2)))
		// 记录死信 - 与原代码保持一致
		b.log.Error("记录死信", zap.String("context", logContext), zap.Any("ids", ids2), zap.String("reason", fmt.Sprintf("es bulk failed after recompute: %v", err)))
		return affected, err
	}

	return affected, nil
}
