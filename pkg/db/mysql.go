package db

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

// MySQL wraps a sqlx.DB
type MySQL struct {
	DB             *sqlx.DB
	Log            *zap.Logger
	DebugSQL       bool
	DebugSQLParams bool
}

// GetMinMax returns MIN(keyCol), MAX(keyCol) from the given table.
func (m *MySQL) GetMinMax(ctx context.Context, table, keyCol string) (int64, int64, error) {
	query := fmt.Sprintf("SELECT COALESCE(MIN(%s),0) AS min_id, COALESCE(MAX(%s),0) AS max_id FROM %s", keyCol, keyCol, table)
	row := m.DB.QueryRowxContext(ctx, query)
	var minID, maxID sql.NullInt64
	if err := row.Scan(&minID, &maxID); err != nil {
		return 0, 0, err
	}
	var a, b int64
	if minID.Valid {
		a = minID.Int64
	}
	if maxID.Valid {
		b = maxID.Int64
	}
	return a, b, nil
}

// NewMySQL opens a new connection.
func NewMySQL(dsn string) (*MySQL, error) {
	// 为避免连接长时间无响应，自动注入连接/读写超时（若未设置）
	if !strings.Contains(dsn, "timeout=") {
		if strings.Contains(dsn, "?") {
			dsn += "&timeout=5s"
		} else {
			dsn += "?timeout=5s"
		}
	}
	if !strings.Contains(dsn, "readTimeout=") {
		dsn += "&readTimeout=5s"
	}
	if !strings.Contains(dsn, "writeTimeout=") {
		dsn += "&writeTimeout=5s"
	}

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// 使用带超时的 Ping，避免卡住不返回
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return nil, err
	}
	return &MySQL{DB: db}, nil
}

// SetLogger sets logger for SQL debug outputs.
func (m *MySQL) SetLogger(log *zap.Logger) { m.Log = log }

// SetDebug enables SQL debug and whether to print parameters.
func (m *MySQL) SetDebug(debugSQL, debugParams bool) {
	m.DebugSQL, m.DebugSQLParams = debugSQL, debugParams
}

// GetIDs returns key list within [start, end) for the given table/key column, limited by maxRows per query.
func (m *MySQL) GetIDs(ctx context.Context, table, keyCol string, start, end int64, maxRows int) ([]int64, error) {
	// NOTE: identifiers cannot be parameterized; ensure table/key come from trusted config.
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? AND %s < ? ORDER BY %s ASC LIMIT ?", keyCol, table, keyCol, keyCol, keyCol)
	rows, err := m.DB.QueryxContext(ctx, query, start, end, maxRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id sql.NullInt64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.Valid {
			ids = append(ids, id.Int64)
		}
	}
	return ids, rows.Err()
}

func (m *MySQL) GetIDsAfter(ctx context.Context, table, keyCol string, after int64, limit int) ([]int64, error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?", keyCol, table, keyCol, keyCol)
	rows, err := m.DB.QueryxContext(ctx, query, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id sql.NullInt64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.Valid {
			ids = append(ids, id.Int64)
		}
	}
	return ids, rows.Err()
}

// QueryMapping executes the main mapping SQL with IN expansion and returns rows as list of maps.
func (m *MySQL) QueryMapping(ctx context.Context, mainSQL string, keys []int64) ([]map[string]interface{}, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	// 统一归一化：将任何 IN(:placeholder) 形式归一为 IN (?)，再使用 sqlx.In 展开
	qTmpl := mainSQL
	if matched, _ := regexp.MatchString(`(?i)IN\s*\(\s*:\w+\s*\)`, qTmpl); matched {
		re := regexp.MustCompile(`(?i)IN\s*\(\s*:\w+\s*\)`)
		qTmpl = re.ReplaceAllString(qTmpl, "IN (?)")
	}
	// use sqlx.In to expand IN clause
	q, args, err := sqlx.In(qTmpl, keys)
	if err != nil {
		return nil, fmt.Errorf("sqlx.In: %w", err)
	}
	q = m.DB.Rebind(q)
	// debug: log final SQL shape
	start := time.Now()
	if m.DebugSQL && m.Log != nil {
		// 压缩SQL中的换行与多余空格，仅用于日志展示
		compact := func(s string) string {
			re := regexp.MustCompile(`\s+`)
			return strings.TrimSpace(re.ReplaceAllString(s, " "))
		}
		qlog := compact(q)
		if m.DebugSQLParams {
			// 打印参数示例，避免过长
			max := len(args)
			if max > 10 {
				max = 10
			}
			m.Log.Debug("exec mapping sql", zap.String("query", qlog), zap.Int("args_total", len(args)), zap.Any("args_sample", args[:max]))
		} else {
			m.Log.Debug("exec mapping sql", zap.String("query", qlog), zap.Int("args_total", len(args)))
		}
	}
	rows, err := m.DB.QueryxContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]map[string]interface{}, 0, len(keys))
	for rows.Next() {
		m := map[string]interface{}{}
		if err := rows.MapScan(m); err != nil {
			return nil, err
		}
		results = append(results, m)
	}
	elapsed := time.Since(start)
	if m.DebugSQL && m.Log != nil {
		m.Log.Debug("exec mapping done", zap.Int("rows", len(results)), zap.Duration("elapsed", elapsed))
	}
	// metrics: sql latency
	metrics.SQLLatencySeconds.Observe(elapsed.Seconds())
	return results, rows.Err()
}
