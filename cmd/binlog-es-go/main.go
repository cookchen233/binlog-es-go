package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	bspkg "github.com/cookchen233/binlog-es-go/pkg/bootstrap"
	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	"github.com/cookchen233/binlog-es-go/pkg/db"
	espkg "github.com/cookchen233/binlog-es-go/pkg/es"
	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	realtime "github.com/cookchen233/binlog-es-go/pkg/realtime"
	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var (
	cfgPath string
	mode    string
	task    string

	// bootstrap params
	bsMinAutoID   int64
	bsMaxAutoID   int64
	bsPartitionSz int
	bsWorkers     int
	bsSQLWhere    string
	bulkSize      int
	bulkFlushMs   int

	// server params
	flagServerPort int

	// logging override
	flagLogLevel string
	flagLogFile  string

	logger *zap.Logger
)

var ()

func init() {
	flag.StringVar(&cfgPath, "config", "configs/config.yaml", "path to config file")
	flag.StringVar(&mode, "mode", "realtime", "run mode: realtime | bootstrap")
	flag.StringVar(&task, "task", "", "task destination name to run (e.g. <task_name>)")

	flag.Int64Var(&bsMinAutoID, "bootstrap.partition.auto_id.min", 0, "bootstrap min AutoID")
	flag.Int64Var(&bsMaxAutoID, "bootstrap.partition.auto_id.max", 0, "bootstrap max AutoID")
	flag.IntVar(&bsPartitionSz, "bootstrap.partition.size", 5000, "bootstrap partition size")
	flag.IntVar(&bsWorkers, "bootstrap.workers", 4, "bootstrap workers")
	flag.StringVar(&bsSQLWhere, "bootstrap.sql-where", "", "extra WHERE for bootstrap main SQL (without 'AND')")

	flag.IntVar(&bulkSize, "bulk.size", 1000, "override bulk size")
	flag.IntVar(&bulkFlushMs, "bulk.flush-ms", 800, "override bulk flush interval ms")

	flag.IntVar(&flagServerPort, "server.port", 0, "override http server port (0 = use config or default 8222)")

	flag.StringVar(&flagLogLevel, "log.level", "", "override logging level: debug|info|warn|error")
	flag.StringVar(&flagLogFile, "log.file", "", "override logging file path")
}

// runSelfCheck validates connectivity and basic requirements for MySQL and ES.
func runSelfCheck() int {
	cfg := cfgpkg.Get()

	// Check MySQL connectivity
	logger.Info("self-check: mysql connect", zap.String("dsn", "***masked***"))
	mysql, err := db.NewMySQL(cfg.DataSource.DSN)
	if err != nil {
		logger.Error("self-check: mysql connect failed", zap.Error(err))
		return 1
	}
	defer mysql.DB.Close()
	logger.Info("self-check: mysql ok")

	// MySQL binlog format & GTID checks
	var binlogFmt string
	_ = mysql.DB.Get(&binlogFmt, "SELECT @@global.binlog_format")
	if strings.ToUpper(binlogFmt) != "ROW" {
		logger.Warn("self-check: binlog_format is not ROW", zap.String("binlog_format", binlogFmt))
	} else {
		logger.Info("self-check: binlog_format ROW")
	}
	var gtidMode string
	_ = mysql.DB.Get(&gtidMode, "SELECT @@global.gtid_mode")
	logger.Info("self-check: gtid_mode", zap.String("gtid_mode", gtidMode), zap.Bool("cfg.gtidEnabled", cfg.DataSource.GTID))

	// binlog_row_image affects whether DELETE rows carry full columns (e.g., SheetID) for child tables
	var rowImage string
	if err := mysql.DB.Get(&rowImage, "SELECT @@global.binlog_row_image"); err == nil {
		logger.Info("self-check: binlog_row_image", zap.String("binlog_row_image", rowImage))
		if strings.EqualFold(rowImage, "MINIMAL") {
			logger.Info("self-check: recommendation", zap.String("tip", "For child-table DELETE to always trigger recompute via direct key in binlog, consider setting binlog_row_image=FULL"))
		}
	}

	// Check ES connectivity
	logger.Info("self-check: es connect", zap.Strings("addresses", cfg.ES.Addresses))
	esw, err := espkg.New(cfg.ES.Addresses, cfg.ES.Username, cfg.ES.Password)
	if err != nil {
		logger.Error("self-check: es client init failed", zap.Error(err))
		return 1
	}
	if err := esw.Ping(context.Background()); err != nil {
		logger.Error("self-check: es ping failed", zap.Error(err))
		return 1
	}
	logger.Info("self-check: es ok")

	// Recreate a plain ES writer to use helper methods (through runner already created)
	// Basic checks per task
	re := regexp.MustCompile("(?i)from\\s+`?([\\w\\.]+)`?\\s*")
	okAll := true
	for _, t := range cfg.SyncTasks {
		// parse main table from SQL
		m := re.FindStringSubmatch(t.Mapping.SQL)
		mainTable := ""
		if len(m) >= 2 {
			mainTable = m[1]
			if strings.Contains(mainTable, ".") {
				parts := strings.SplitN(mainTable, ".", 2)
				mainTable = parts[1]
			}
		}
		// 统一小写，匹配 mappingTable 的键
		mainTable = strings.ToLower(mainTable)
		logger.Info("self-check: task", zap.String("task", t.Destination), zap.String("main_table", mainTable))

		// verify mappingTable contains main table and key
		key := ""
		if arr, ok := t.MappingTable[mainTable]; ok && len(arr) > 0 {
			key = arr[0]
		}
		if key == "" {
			logger.Error("self-check: mappingTable missing or key empty", zap.String("task", t.Destination), zap.String("main_table", mainTable))
			okAll = false
			continue
		}
		// quick min/max to verify permissions
		if _, _, err := mysql.GetMinMax(context.Background(), mainTable, key); err != nil {
			logger.Error("self-check: mysql min/max failed", zap.String("table", mainTable), zap.String("key", key), zap.Error(err))
			okAll = false
		} else {
			logger.Info("self-check: mysql min/max ok", zap.String("table", mainTable), zap.String("key", key))
		}
		// index exists check
		exists, err := esw.IndexExists(context.Background(), t.Mapping.Index)
		if err != nil {
			logger.Error("self-check: es index exists check failed", zap.String("index", t.Mapping.Index), zap.Error(err))
			okAll = false
		} else if !exists {
			logger.Warn("self-check: es index not found", zap.String("index", t.Mapping.Index))
		} else {
			logger.Info("self-check: es index exists", zap.String("index", t.Mapping.Index))
		}
	}

	if !okAll {
		return 1
	}
	logger.Info("self-check: all passed")
	return 0
}

func mustInitLogger() *zap.Logger {
	// 初始使用开发日志，加载配置后会按需重建
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return l
}

func rebuildLoggerFromConfig(c cfgpkg.Config) (*zap.Logger, error) {
	level := zapcore.InfoLevel
	switch c.Logging.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	}

	encCfg := zap.NewDevelopmentEncoderConfig()
	encCfg.TimeKey = "time"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(encCfg), zapcore.AddSync(os.Stdout), level)

	var cores []zapcore.Core
	cores = append(cores, consoleCore)

	if c.Logging.File != "" {
		lj := &lumberjack.Logger{
			Filename:   c.Logging.File,
			MaxSize:    50, // MB
			MaxBackups: 7,
			MaxAge:     14, // days
			Compress:   true,
		}
		fileCore := zapcore.NewCore(zapcore.NewJSONEncoder(encCfg), zapcore.AddSync(lj), level)
		cores = append(cores, fileCore)
	}

	logger := zap.New(zapcore.NewTee(cores...), zap.AddCaller(), zap.AddCallerSkip(1))
	return logger, nil
}

func loadConfig(path string) error {
	viper.SetConfigFile(path)
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return nil
}

func runBootstrap() int {
	cfg := cfgpkg.Get()
	if task == "" {
		logger.Error("bootstrap requires --task")
		return 2
	}
	// Apply defaults from config if flags are not provided
	if bsPartitionSz <= 0 && cfg.Bootstrap.PartitionSize > 0 {
		bsPartitionSz = cfg.Bootstrap.PartitionSize
	}
	if bsWorkers <= 0 && cfg.Bootstrap.Workers > 0 {
		bsWorkers = cfg.Bootstrap.Workers
	}
	if bulkSize <= 0 && cfg.Bootstrap.BulkSize > 0 {
		bulkSize = cfg.Bootstrap.BulkSize
	}
	if bulkFlushMs <= 0 && cfg.Bootstrap.BulkFlushMs > 0 {
		bulkFlushMs = cfg.Bootstrap.BulkFlushMs
	}

	// 立即输出一条标准输出，确保在日志系统之前也能看到启动信息
	fmt.Println("[BOOTSTRAP] starting... applying defaults and selecting task")
	logger.Info("bootstrap starting",
		zap.String("task", task),
		zap.Int64("min_auto_id", bsMinAutoID),
		zap.Int64("max_auto_id", bsMaxAutoID),
		zap.Int("partition_size", bsPartitionSz),
		zap.Int("workers", bsWorkers),
		zap.String("sql_where", bsSQLWhere),
		zap.Int("bulk.size", bulkSize),
		zap.Int("bulk.flush_ms", bulkFlushMs),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 捕获退出信号，优雅停止（runner 内部会在退出前flush并保存位点）
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		logger.Warn("signal received, shutting down bootstrap", zap.String("signal", s.String()))
		cancel()
	}()

	// call bootstrap runner
	// find task by destination
	var selected *cfgpkg.SyncTask
	for i := range cfg.SyncTasks {
		if cfg.SyncTasks[i].Destination == task {
			selected = &cfg.SyncTasks[i]
			break
		}
	}
	if selected == nil {
		logger.Error("task not found in config", zap.String("task", task))
		return 2
	}
	// 应用 CLI 覆盖到任务的 bulk 配置（仅当 >0 时）
	if bulkSize > 0 {
		selected.Bulk.Size = bulkSize
	}
	if bulkFlushMs > 0 {
		selected.Bulk.FlushIntervalMs = bulkFlushMs
	}
	runner, err := bspkg.NewRunner(logger, cfg, *selected)
	if err != nil {
		logger.Error("init bootstrap runner failed", zap.Error(err))
		return 1
	}
	// If min/max not provided, auto-detect from main table (PoC)
	if bsMinAutoID == 0 || bsMaxAutoID == 0 {
		minV, maxV, errRange := runner.ResolveAutoIDRange(ctx)
		if errRange != nil {
			logger.Error("resolve auto id range failed", zap.Error(errRange))
			return 1
		}
		if bsMinAutoID == 0 {
			bsMinAutoID = minV
		}
		if bsMaxAutoID == 0 {
			bsMaxAutoID = maxV
		}
	}

	err = runner.Run(ctx, bsMinAutoID, bsMaxAutoID, bsPartitionSz, bsWorkers, bsSQLWhere)
	if err != nil {
		logger.Error("bootstrap failed", zap.Error(err))
		return 1
	}
	logger.Info("bootstrap finished", zap.Time("time", time.Now()))
	return 0
}

func runRealtime() int {
	cfg := cfgpkg.Get()
	if task == "" {
		logger.Error("realtime requires --task")
		return 2
	}
	// find task by destination
	var selected *cfgpkg.SyncTask
	for i := range cfg.SyncTasks {
		if cfg.SyncTasks[i].Destination == task {
			selected = &cfg.SyncTasks[i]
			break
		}
	}
	if selected == nil {
		logger.Error("task not found in config", zap.String("task", task))
		return 2
	}

	logger.Info("realtime starting", zap.String("task", task))

	// start runner
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 捕获退出信号，优雅停止（runner 内部会在退出前flush并保存位点）
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		logger.Warn("signal received, shutting down realtime", zap.String("signal", s.String()))
		cancel()
	}()

	rnr, err := bspkg.NewRunner(logger, cfg, *selected)
	if err != nil {
		logger.Error("init bootstrap runner failed (for shared deps)", zap.Error(err))
		return 1
	}
	_ = rnr // ensure deps initialized if needed

	// use dedicated realtime runner
	rr, err := realtime.NewRunner(logger, cfg)
	if err != nil {
		logger.Error("init realtime runner failed", zap.Error(err))
		return 1
	}
	if err := rr.Run(ctx, *selected); err != nil {
		logger.Error("realtime run error", zap.Error(err))
		return 1
	}
	return 0
}

func main() {
	flag.Parse()

	logger = mustInitLogger()
	defer logger.Sync() //nolint:errcheck

	if err := loadConfig(cfgPath); err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(2)
	}

	if _, err := cfgpkg.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to unmarshal config: %v\n", err)
		os.Exit(2)
	}

	// 重建日志：根据配置(logging.level, logging.file)
	// 覆盖配置的日志参数
	cfg := cfgpkg.Get()
	if flagLogLevel != "" {
		cfg.Logging.Level = flagLogLevel
	}
	if flagLogFile != "" {
		cfg.Logging.File = flagLogFile
	}
	if l2, err := rebuildLoggerFromConfig(cfg); err == nil {
		_ = logger.Sync()
		logger = l2
	} else {
		fmt.Fprintf(os.Stderr, "failed to rebuild logger: %v\n", err)
	}

	logger.Info("binlog-es-go starting",
		zap.String("config", cfgPath),
		zap.String("mode", mode),
	)

	// Start internal HTTP server (/healthz, /metrics)
	// 判断是否需要启动 HTTP 服务器
	isShortTask := mode == "bootstrap" || mode == "replay-deadletters" || mode == "self-check"
	skipHTTPServer := false

	if isShortTask && cfg.Server.DisableForShortTasks {
		skipHTTPServer = true
		logger.Info("http server disabled for short-running task", zap.String("mode", mode))
	}

	if !skipHTTPServer {
		metrics.Register()
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			metrics.Requests.Inc()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		mux.Handle("/metrics", promhttp.Handler())
		// 端口选择优先级: 命令行参数 > 模式专用端口 > 通用端口 > 默认值
		srvPort := 0
		if flagServerPort > 0 {
			// 命令行参数优先级最高
			srvPort = flagServerPort
		} else {
			// 根据模式选择对应端口
			switch mode {
			case "realtime":
				if cfg.Server.RealtimePort > 0 {
					srvPort = cfg.Server.RealtimePort
				}
			case "bootstrap":
				if cfg.Server.BootstrapPort > 0 {
					srvPort = cfg.Server.BootstrapPort
				}
			case "replay-deadletters":
				if cfg.Server.ReplayDeadLettersPort > 0 {
					srvPort = cfg.Server.ReplayDeadLettersPort
				}
			}
			// 如果模式专用端口未配置，使用通用端口
			if srvPort == 0 {
				srvPort = cfg.Server.Port
			}
		}
		// 最终默认值
		if srvPort == 0 {
			srvPort = 8222
		}
		srv := &http.Server{Addr: fmt.Sprintf(":%d", srvPort), Handler: mux}
		go func() {
			logger.Info("http server listening", zap.Int("port", srvPort), zap.String("mode", mode))
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Warn("http server error", zap.Error(err))
			}
		}()
	}

	switch mode {
	case "bootstrap":
		code := runBootstrap()
		os.Exit(code)
	case "realtime":
		code := runRealtime()
		os.Exit(code)
	case "self-check":
		code := runSelfCheck()
		os.Exit(code)
	case "replay-deadletters":
		code := runReplayDeadLetters()
		os.Exit(code)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", mode)
		os.Exit(2)
	}
}

// runReplayDeadLetters reads logs/dead-letters/<task>.log, parses IDs and runs bootstrap RunWithIDs once.
func runReplayDeadLetters() int {
	cfg := cfgpkg.Get()
	if task == "" {
		logger.Error("replay-deadletters requires --task")
		return 2
	}
	// locate task
	var selected *cfgpkg.SyncTask
	for i := range cfg.SyncTasks {
		if cfg.SyncTasks[i].Destination == task {
			selected = &cfg.SyncTasks[i]
			break
		}
	}
	if selected == nil {
		logger.Error("task not found in config", zap.String("task", task))
		return 2
	}
	// Build runner
	runner, err := bspkg.NewRunner(logger, cfg, *selected)
	if err != nil {
		logger.Error("init bootstrap runner failed", zap.Error(err))
		return 1
	}
	// Read dead-letters file
	path := filepath.Join("logs", "dead-letters", task+".log")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warn("dead-letters file not found", zap.String("path", path))
			return 0
		}
		logger.Error("open dead-letters failed", zap.Error(err), zap.String("path", path))
		return 1
	}
	defer f.Close()
	ids := parseIDsFromDeadLetters(f)
	if len(ids) == 0 {
		logger.Info("no ids parsed from dead-letters", zap.String("path", path))
		return 0
	}
	logger.Info("replay dead-letters", zap.Int("ids", len(ids)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runner.RunWithIDs(ctx, ids); err != nil {
		logger.Error("replay dead-letters failed", zap.Error(err))
		return 1
	}
	// Move processed file to processed directory with timestamp suffix
	procDir := filepath.Join("logs", "dead-letters", "processed")
	_ = os.MkdirAll(procDir, 0o755)
	newPath := filepath.Join(procDir, fmt.Sprintf("%s-%d.log", task, time.Now().Unix()))
	if err := moveFile(path, newPath); err != nil {
		logger.Warn("archive dead-letters failed", zap.Error(err), zap.String("from", path), zap.String("to", newPath))
	} else {
		logger.Info("dead-letters archived", zap.String("to", newPath))
	}
	return 0
}

// parseIDsFromDeadLetters parses lines like: ts|[1 2 3]|reason
func parseIDsFromDeadLetters(r io.Reader) []int64 {
	scanner := bufio.NewScanner(r)
	// increase buffer for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	idset := make(map[int64]struct{})
	re := regexp.MustCompile(`\[(.*?)\]`)
	for scanner.Scan() {
		line := scanner.Text()
		m := re.FindStringSubmatch(line)
		if len(m) < 2 {
			continue
		}
		inner := strings.TrimSpace(m[1])
		if inner == "" {
			continue
		}
		// split by space or comma
		parts := strings.FieldsFunc(inner, func(r rune) bool { return r == ' ' || r == ',' })
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if v, err := strconv.ParseInt(p, 10, 64); err == nil {
				idset[v] = struct{}{}
			}
		}
	}
	out := make([]int64, 0, len(idset))
	for v := range idset {
		out = append(out, v)
	}
	return out
}

func moveFile(from, to string) error {
	// try to rename first
	if err := os.Rename(from, to); err == nil {
		return nil
	}
	// fallback copy+remove
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return os.Remove(from)
}
