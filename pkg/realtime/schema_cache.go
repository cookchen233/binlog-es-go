package realtime

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"sync"
)

// schemaCache caches column order for a table
// moved out of runner.go to reduce file size and improve cohesion
// key format: schema.table
// NOTE: exported only within package

type schemaCache struct {
	mu   sync.RWMutex
	cols map[string][]string // key: schema.table
}

func newSchemaCache() *schemaCache { return &schemaCache{cols: make(map[string][]string)} }

func (c *schemaCache) get(db *sqlx.DB, schema, table string) ([]string, error) {
	key := schema + "." + table
	c.mu.RLock()
	if v, ok := c.cols[key]; ok {
		c.mu.RUnlock()
		return v, nil
	}
	c.mu.RUnlock()
	q := `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`
	rows, err := db.Queryx(q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name sql.NullString
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name.Valid {
			names = append(names, name.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.cols[key] = names
	c.mu.Unlock()
	return names, nil
}

// no extra types
