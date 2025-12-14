package mapper

import (
	"context"
	"time"

	"github.com/cookchen233/binlog-es-go/pkg/util"
	"go.uber.org/zap"
)

// RetryConfig defines retry attempts and backoff in milliseconds.
type RetryConfig struct {
	MaxAttempts int
	BackoffMs   []int
}

// Do execute the provided query function with timeout and retry, and returns rows and elapsed time.
// The query function should honor the passed context for timeout/cancellation.
func Do(
	ctx context.Context,
	timeout time.Duration,
	retry RetryConfig,
	log *zap.Logger,
	query func(qctx context.Context) ([]map[string]interface{}, error),
) (rows []map[string]interface{}, elapsed time.Duration, err error) {
	start := time.Now()
	err = util.Retry(retry.MaxAttempts, retry.BackoffMs, func() error {
		qctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		var e error
		rows, e = query(qctx)
		if e != nil && log != nil {
			log.Warn("mapping query retry", zap.Error(e))
		}
		return e
	}, nil)
	elapsed = time.Since(start)
	return
}
