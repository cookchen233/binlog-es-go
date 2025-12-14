package config

// Config is the root config loaded from YAML via viper.
type Config struct {
	DataSource DataSource `mapstructure:"dataSource"`
	ES         ES         `mapstructure:"es"`
	Position   Position   `mapstructure:"position"`
	Bootstrap  Bootstrap  `mapstructure:"bootstrap"`
	Logging    Logging    `mapstructure:"logging"`
	Debug      Debug      `mapstructure:"debug"`
	Server     Server     `mapstructure:"server"`
	Realtime   Realtime   `mapstructure:"realtime"`
	SyncTasks  []SyncTask `mapstructure:"syncTasks"`
}

type DataSource struct {
	DSN      string `mapstructure:"dsn"`
	ServerID int    `mapstructure:"serverId"`
	GTID     bool   `mapstructure:"gtidEnabled"`
	Flavor   string `mapstructure:"flavor"`
}

type ES struct {
	Addresses []string `mapstructure:"addresses"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
	Version   int      `mapstructure:"version"`
	Refresh   string   `mapstructure:"refresh"` // "", "false", "wait_for", "true"
}

type Position struct {
	Path    string `mapstructure:"path"`
	UseGTID bool   `mapstructure:"useGTID"`
}

// Bootstrap default parameters when CLI flags are omitted (zero values)
type Bootstrap struct {
	PartitionSize int `mapstructure:"partitionSize"`
	Workers       int `mapstructure:"workers"`
	BulkSize      int `mapstructure:"bulkSize"`
	BulkFlushMs   int `mapstructure:"bulkFlushMs"`
	RunBatchSize  int `mapstructure:"runBatchSize"` // RunWithIDs 内层查询/写入批尺寸（优先于默认1000）
}

type Logging struct {
	Level string `mapstructure:"level"` // debug, info, warn, error
	File  string `mapstructure:"file"`  // optional file path, e.g. logs/app.log
}

// Debug controls additional verbose outputs useful during development/testing.
type Debug struct {
	SQL       bool `mapstructure:"sql"`       // print final SQL shape
	SQLParams bool `mapstructure:"sqlParams"` // whether to print SQL param values (consider masking)
}

// Server config for embedded HTTP endpoints (/healthz, /metrics)
type Server struct {
	Port int `mapstructure:"port"` // 默认端口（所有模式共用，如未单独配置）
	// 各模式独立端口配置（优先级高于 Port）
	RealtimePort          int  `mapstructure:"realtimePort"`          // realtime 模式端口
	BootstrapPort         int  `mapstructure:"bootstrapPort"`         // bootstrap 模式端口（0 表示禁用）
	ReplayDeadLettersPort int  `mapstructure:"replayDeadLettersPort"` // replay-deadletters 模式端口（0 表示禁用）
	DisableForShortTasks  bool `mapstructure:"disableForShortTasks"`  // 是否对短期任务（bootstrap/replay-deadletters）禁用 HTTP 服务器
}

// Realtime config for binlog reader behavior
type Realtime struct {
	HeartbeatMs   int `mapstructure:"heartbeatMs"`   // go-mysql HeartbeatPeriod，毫秒
	ReadTimeoutMs int `mapstructure:"readTimeoutMs"` // go-mysql ReadTimeout，毫秒
	MaxPending    int `mapstructure:"maxPending"`    // 防止内存积压的最大pending数量，达到后强制flush
	// 运行超时与容错
	QueryTimeoutMs         int `mapstructure:"queryTimeoutMs"`         // 主SQL查询超时，默认30000
	ESBulkTimeoutMs        int `mapstructure:"esBulkTimeoutMs"`        // ES批量写入超时，默认60000
	ESCircuitMaxBackoffMs  int `mapstructure:"esCircuitMaxBackoffMs"`  // ES熔断指数退避上限，默认30000
	RelatedQueryMaxPages   int `mapstructure:"relatedQueryMaxPages"`   // relatedQuery分页最大页数，默认1000
	PositionSaveIntervalMs int `mapstructure:"positionSaveIntervalMs"` // 兜底保存位点的周期，默认30000
}

type SyncTask struct {
	Destination string  `mapstructure:"destination"`
	Mapping     Mapping `mapstructure:"mapping"`
	// Optional: rewrite physical table names from binlog events to logical names for routing.
	// Example: enterprise_00~63 -> enterprise
	TableRewrite []TableRewriteRule `mapstructure:"tableRewrite"`
	// Optional tune settings
	Bulk  Bulk  `mapstructure:"bulk"`
	Retry Retry `mapstructure:"retry"`
	// Table routing and reverse lookup live at task level per YAML
	MappingTable map[string][]string         `mapstructure:"mappingTable"`
	RelatedQuery map[string]RelatedQueryRule `mapstructure:"relatedQuery"`
	// Optional transforms for documents before writing to ES
	Transforms Transforms `mapstructure:"transforms"`
}

// TableRewriteRule rewrites incoming binlog table names for routing purposes.
// Pattern is a regexp (case-insensitive is recommended) matched against the physical table name.
// Replace is the replacement string.
type TableRewriteRule struct {
	Pattern string `mapstructure:"pattern"`
	Replace string `mapstructure:"replace"`
}

type Mapping struct {
	Index  string `mapstructure:"_index"`
	ID     string `mapstructure:"_id"`
	Upsert bool   `mapstructure:"upsert"`
	SQL    string `mapstructure:"sql"`
	// Optional: explicitly declare logical main table name used for binlog routing.
	// Useful when SQL contains template variables (e.g. FROM {{enterprise_table}}).
	MainTable string `mapstructure:"mainTable"`
	// Optional sharding config for main SQL execution. When enabled, keys are grouped by shard,
	// and SQL template variables are rendered per shard (e.g. {{enterprise_table}} -> enterprise_00).
	Sharding *ShardingConfig `mapstructure:"sharding"`
	// When true, if main SQL returns no rows for incoming keys, delete those IDs from ES
	DeleteOnMissing bool `mapstructure:"deleteOnMissing"`
	// When true, on main table delete binlog events, delete corresponding IDs from ES directly
	DeleteOnDelete bool `mapstructure:"deleteOnDelete"`
}

// ShardingConfig describes how to route keys to physical shard tables.
// Vars maps template variable name -> base table name. For each batch shard, variables will be
// rendered as <base>_<suffix> (suffix is zero padded).
// Example: vars: { enterprise_table: enterprise, enterprise_event_table: enterprise_event }
type ShardingConfig struct {
	// Strategy decides how to compute shard for a key.
	// - "mod" (default): abs(key) % shards
	// - "crc32": crc32(string(key)) % shards (compatible with mysaas ShardRouter)
	Strategy    string            `mapstructure:"strategy"`
	Shards      int               `mapstructure:"shards"`
	SuffixWidth int               `mapstructure:"suffixWidth"`
	Vars        map[string]string `mapstructure:"vars"`
}

type RelatedQueryRule struct {
	SQL      string            `mapstructure:"sql"`
	Bind     map[string]string `mapstructure:"bind"`
	PageSize int               `mapstructure:"pageSize"` // 可选：当SQL包含 :last_id 和 :limit 时，按分页方式拉取
	// 可选：声明式分页配置（更直观）。当 Mode="cursor" 时，忽略 SQL，由程序拼接：
	// SELECT AutoID FROM <TargetTable|<main_table>> WHERE <Filter> AND <OrderKey> > :cursor ORDER BY <OrderKey> ASC LIMIT :page_size
	Mode        string `mapstructure:"mode"`        // "cursor" 使用游标分页
	TargetTable string `mapstructure:"targetTable"` // 目标表（留空时默认使用主表）
	Filter      string `mapstructure:"filter"`      // 过滤条件，如 "Category_id = :category_id"
	OrderKey    string `mapstructure:"orderKey"`    // 游标字段，如 "AutoID"
}

// Transforms defines optional doc transformations prior to ES upsert
type Transforms struct {
	SplitFields []SplitFieldRule `mapstructure:"splitFields"`
}

// SplitFieldRule splits a string field into an array by separator
type SplitFieldRule struct {
	Field string `mapstructure:"field"`
	Sep   string `mapstructure:"sep"`  // default ";"
	Trim  bool   `mapstructure:"trim"` // default true
}

type Bulk struct {
	Size            int `mapstructure:"size"`
	FlushIntervalMs int `mapstructure:"flushIntervalMs"`
	Concurrent      int `mapstructure:"concurrent"`
}

type Retry struct {
	MaxAttempts int   `mapstructure:"maxAttempts"`
	BackoffMs   []int `mapstructure:"backoffMs"`
}
