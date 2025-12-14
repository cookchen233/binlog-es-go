package metrics

import (
	"sync"
	prom "github.com/prometheus/client_golang/prometheus"
)

var (
	Requests = prom.NewCounter(prom.CounterOpts{
		Name: "binlog_es_sync_http_requests_total",
		Help: "Total HTTP requests served by internal server.",
	})

	RealtimeBinlogLagSeconds = prom.NewGauge(prom.GaugeOpts{
		Name: "binlog_es_sync_realtime_binlog_lag_seconds",
		Help: "Estimated lag between now and last binlog event timestamp (seconds).",
	})

	RealtimeFlushBatchSize = prom.NewHistogram(prom.HistogramOpts{
		Name:    "binlog_es_sync_realtime_flush_batch_size",
		Help:    "Batch size of each realtime flush.",
		Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000},
	})

	SQLLatencySeconds = prom.NewHistogram(prom.HistogramOpts{
		Name:    "binlog_es_sync_sql_latency_seconds",
		Help:    "Latency of mapping SQL execution.",
		Buckets: prom.DefBuckets,
	})

	ESBulkLatencySeconds = prom.NewHistogram(prom.HistogramOpts{
		Name:    "binlog_es_sync_es_bulk_latency_seconds",
		Help:    "Latency of ES bulk upsert.",
		Buckets: prom.DefBuckets,
	})

	RetryTotal = prom.NewCounterVec(prom.CounterOpts{
		Name: "binlog_es_sync_retry_total",
		Help: "Total retries by component.",
	}, []string{"component"})

	ReconnectTotal = prom.NewCounter(prom.CounterOpts{
		Name: "binlog_es_sync_reconnect_total",
		Help: "Total reconnect attempts in realtime streamer.",
	})

	// 新增：成功写入 ES 的文档数
	UpsertSuccessTotal = prom.NewCounter(prom.CounterOpts{
		Name: "binlog_es_sync_upsert_success_total",
		Help: "Total number of documents successfully upserted to Elasticsearch.",
	})

	// 新增：成功从 ES 删除的文档数（按原因分类）
	DeleteSuccessTotal = prom.NewCounterVec(prom.CounterOpts{
		Name: "binlog_es_sync_delete_success_total",
		Help: "Total number of documents successfully deleted from Elasticsearch.",
	}, []string{"reason"})

	// 新增：死信计数
	DeadLettersTotal = prom.NewCounter(prom.CounterOpts{
		Name: "binlog_es_sync_dead_letters_total",
		Help: "Total number of IDs written to dead-letters.",
	})
)

func Register() {
    regOnce.Do(func() {
        // 仅注册本项目自定义指标，避免与外部已注册的默认采集器冲突
        prom.MustRegister(Requests)
        prom.MustRegister(RealtimeBinlogLagSeconds)
        prom.MustRegister(RealtimeFlushBatchSize)
        prom.MustRegister(SQLLatencySeconds)
        prom.MustRegister(ESBulkLatencySeconds)
        prom.MustRegister(RetryTotal)
        prom.MustRegister(ReconnectTotal)
        prom.MustRegister(UpsertSuccessTotal)
        prom.MustRegister(DeleteSuccessTotal)
        prom.MustRegister(DeadLettersTotal)
    })
}

var regOnce sync.Once
