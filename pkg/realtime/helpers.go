package realtime

import (
	"fmt"
	"hash/crc32"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

// parseForReplication parses DSN and returns user/pass/host/port for binlog syncer config.
func parseForReplication(dsn string) (user, pass, host string, port uint16, err error) {
	cfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return
	}

	user = cfg.User
	pass = cfg.Passwd
	addr := cfg.Addr
	if strings.Contains(addr, ":") {
		parts := strings.Split(addr, ":")
		host = parts[0]
		var p int
		fmt.Sscanf(parts[1], "%d", &p)
		port = uint16(p)
	} else {
		host = addr
		port = 3306
	}
	return
}

// DedupInt64 returns a slice with duplicates removed while preserving order of first appearance.
func DedupInt64(ids []int64) []int64 {
	if len(ids) <= 1 {
		return ids
	}
	m := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, v := range ids {
		if _, ok := m[v]; ok {
			continue
		}
		m[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// rewriteTableName applies configured rewrite rules to the physical table name and returns the logical name.
// If no rule matches, returns the original table name.
func rewriteTableName(table string, rules []cfgpkg.TableRewriteRule) string {
	if table == "" || len(rules) == 0 {
		return table
	}
	for _, rule := range rules {
		if strings.TrimSpace(rule.Pattern) == "" {
			continue
		}
		re, err := regexp.Compile(rule.Pattern)
		if err != nil {
			continue
		}
		if re.MatchString(table) {
			return re.ReplaceAllString(table, rule.Replace)
		}
	}
	return table
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

// renderSQLTemplate replaces {{var}} occurrences based on shard config.
// Vars maps template var -> base table, then we render base + "_" + zero-padded suffix.
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
		strategy = "mod"
	}

	switch strategy {
	case "crc32":
		// Match mysaas ShardRouter: crc32((string)key) % shards
		// Note: strconv.FormatInt always produces valid UTF-8.
		s := strconv.FormatInt(key, 10)
		if !utf8.ValidString(s) {
			// extremely defensive; should never happen
			strategy = "mod"
			break
		}
		h := crc32.ChecksumIEEE([]byte(s))
		sh := int(h % uint32(sharding.Shards))
		if sh < 0 {
			sh = -sh
		}
		return sh
	default:
		if key < 0 {
			key = -key
		}
		return int(key % int64(sharding.Shards))
	}

	if key < 0 {
		key = -key
	}
	return int(key % int64(sharding.Shards))
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

// getMainTableName tries to parse the main table from the mapping SQL's FROM clause.
func getMainTableName(sql string) string {
	// naive regexp: FROM\s+`?([\w\.]+)`?\s*
	re := regexp.MustCompile("(?i)from\\s+`?([\\w\\.]+)`?\\s*")
	m := re.FindStringSubmatch(sql)
	if len(m) >= 2 {
		t := m[1]
		// strip schema prefix if exists
		if strings.Contains(t, ".") {
			parts := strings.SplitN(t, ".", 2)
			t = parts[1]
		}
		return strings.ToLower(t)
	}
	return ""
}
