package realtime

import (
	"context"
	"strings"
	"time"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	"github.com/cookchen233/binlog-es-go/pkg/db"
	"github.com/cookchen233/binlog-es-go/pkg/es"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/sink"
	"github.com/cookchen233/binlog-es-go/pkg/position"
	"github.com/cookchen233/binlog-es-go/pkg/tracking"
	mysqlpkg "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"
)

// Runner for realtime binlog -> SQL -> ES
type Runner struct {
	log            *zap.Logger
	cfg            cfgpkg.Config
	db             *db.MySQL
	es             *es.Writer
	posStore       *position.Store
	failureTracker *tracking.FailureTracker // 失败追踪器
}

func NewRunner(log *zap.Logger, cfg cfgpkg.Config) (*Runner, error) {
	mdb, err := db.NewMySQL(cfg.DataSource.DSN)
	if err != nil {
		return nil, err
	}
	// 注入日志与SQL调试开关，便于打印最终执行SQL
	mdb.SetLogger(log)
	mdb.SetDebug(cfg.Debug.SQL, cfg.Debug.SQLParams)
	// 避免 GROUP_CONCAT 聚合被默认长度截断，影响 item_specifics 聚合结果
	if _, e := mdb.DB.Exec("SET SESSION group_concat_max_len = 1048576"); e != nil {
		log.Warn("设置group_concat_max_len失败", zap.Error(e))
	} else {
		log.Info("设置group_concat_max_len", zap.Int("value", 1048576))
	}
	esw, err := es.NewWithTLS(cfg.ES.Addresses, cfg.ES.Username, cfg.ES.Password, es.TLSConfig{
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
	ps := position.New(cfg.Position.Path)
	return &Runner{log: log, cfg: cfg, db: mdb, es: esw, posStore: ps}, nil
}

// Run starts the subscription and routes events to SQL->ES
func (r *Runner) Run(ctx context.Context, task cfgpkg.SyncTask) error {
	defer func() {
		if rec := recover(); rec != nil {
			r.log.Error("realtime panic", zap.Any("recover", rec))
		}
	}()
	user, pass, host, port, err := parseForReplication(r.cfg.DataSource.DSN)
	if err != nil {
		return err
	}
	// 读取心跳/读超时配置（毫秒）
	hbMs := r.cfg.Realtime.HeartbeatMs
	if hbMs <= 0 {
		hbMs = 1000
	}
	rtoMs := r.cfg.Realtime.ReadTimeoutMs
	if rtoMs <= 0 {
		rtoMs = 2000
	}
	syncCfg := replication.BinlogSyncerConfig{
		ServerID:   uint32(r.cfg.DataSource.ServerID),
		Flavor:     r.cfg.DataSource.Flavor,
		Host:       host,
		Port:       port,
		User:       user,
		Password:   pass,
		UseDecimal: true,
		ParseTime:  true,
		// 减少空闲期感知延迟：启用心跳与读超时
		HeartbeatPeriod: time.Duration(hbMs) * time.Millisecond,
		ReadTimeout:     time.Duration(rtoMs) * time.Millisecond,
	}
	r.log.Info("realtime reader config", zap.Int("heartbeatMs", hbMs), zap.Int("readTimeoutMs", rtoMs))
	syncer := replication.NewBinlogSyncer(syncCfg)
	defer syncer.Close()

	// load position
	st, err := r.posStore.Load()
	if err != nil {
		return err
	}
	var streamer *replication.BinlogStreamer
	currentFile := st.File
	var lastPos uint32 = st.Pos
	var lastGTID string
	if r.cfg.Position.UseGTID && r.cfg.DataSource.GTID {
		// 优先使用已保存的 GTID 集；为空则从当前 gtid_executed 尾部启动
		gtid := st.GTID
		if gtid == "" {
			var cur string
			if err := r.db.DB.Get(&cur, "SELECT @@global.gtid_executed"); err != nil {
				return err
			}
			gtid = cur
			r.log.Info("starting from current gtid_executed", zap.String("gtid", gtid))
		}
		set, err := mysqlpkg.ParseGTIDSet(r.cfg.DataSource.Flavor, gtid)
		if err != nil {
			return err
		}
		streamer, err = syncer.StartSyncGTID(set)
		if err != nil {
			return err
		}
	} else {
		// file/pos
		file := st.File
		pos := st.Pos
		if file == "" || pos == 0 {
			// start from master status
			var fileName string
			var position uint32
			row := r.db.DB.QueryRowx("SHOW MASTER STATUS")
			var ignore interface{}
			if err := row.Scan(&fileName, &position, &ignore, &ignore, &ignore); err != nil {
				// MySQL 8.4+ may not accept SHOW MASTER STATUS in some setups, fallback to SHOW BINARY LOG STATUS.
				if strings.Contains(strings.ToUpper(err.Error()), "MASTER STATUS") {
					r.log.Warn("SHOW MASTER STATUS failed, trying SHOW BINARY LOG STATUS", zap.Error(err))
					row2 := r.db.DB.QueryRowx("SHOW BINARY LOG STATUS")
					if err2 := row2.Scan(&fileName, &position, &ignore, &ignore, &ignore); err2 != nil {
						return err2
					}
				} else {
					return err
				}
			}
			file, pos = fileName, position
			r.log.Info("starting from master status", zap.String("file", file), zap.Uint32("pos", pos))
		}
		currentFile = file
		lastPos = pos
		streamer, err = syncer.StartSync(mysqlpkg.Position{Name: currentFile, Pos: lastPos})
		if err != nil {
			return err
		}
	}

	schemaCache := newSchemaCache()
	index := task.Mapping.Index
	mainSQL := task.Mapping.SQL
	parsedMainTable := getMainTableName(mainSQL)
	effectiveMainTable := parsedMainTable
	if strings.TrimSpace(task.Mapping.MainTable) != "" {
		effectiveMainTable = strings.ToLower(strings.TrimSpace(task.Mapping.MainTable))
	}
	r.log.Info(
		"realtime mapping info",
		zap.String("index", index),
		zap.String("main_table", effectiveMainTable),
		zap.String("sql_main_table", parsedMainTable),
	)

	// 构建统一的 ES BulkWriter（启用熔断）
	bw := sink.NewBulkWriter(r.es, r.log, sink.Options{
		Retry:          sink.RetryConfig{MaxAttempts: task.Retry.MaxAttempts, BackoffMs: task.Retry.BackoffMs},
		CircuitEnabled: true,
		MaxBackoff:     time.Duration(r.cfg.Realtime.ESCircuitMaxBackoffMs) * time.Millisecond,
	})

	// 读取可配置超时/容错参数
	qTimeout := time.Duration(r.cfg.Realtime.QueryTimeoutMs) * time.Millisecond
	if qTimeout <= 0 {
		qTimeout = 30 * time.Second
	}
	esTimeout := time.Duration(r.cfg.Realtime.ESBulkTimeoutMs) * time.Millisecond
	if esTimeout <= 0 {
		esTimeout = 60 * time.Second
	}
	cbMax := time.Duration(r.cfg.Realtime.ESCircuitMaxBackoffMs) * time.Millisecond
	if cbMax <= 0 {
		cbMax = 30 * time.Second
	}

	// 委托到 runEventLoop 统一处理事件循环与 flush
	return r.runEventLoop(ctx, task, schemaCache, effectiveMainTable, index, mainSQL, bw, syncCfg, syncer, streamer, &currentFile, &lastPos, &lastGTID, qTimeout, esTimeout)
}
