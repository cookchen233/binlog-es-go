package realtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"reflect"
	"regexp"

	cfgpkg "github.com/cookchen233/binlog-es-go/pkg/config"
	metrics "github.com/cookchen233/binlog-es-go/pkg/metrics"
	"github.com/cookchen233/binlog-es-go/pkg/pipeline/sink"
	"github.com/cookchen233/binlog-es-go/pkg/position"
	mysqlpkg "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

// runEventLoop contains the main binlog event processing loop.
func (r *Runner) runEventLoop(
	ctx context.Context,
	task cfgpkg.SyncTask,
	schemaCache *schemaCache,
	mainTable string,
	index string,
	mainSQL string,
	bw *sink.BulkWriter,
	syncCfg replication.BinlogSyncerConfig,
	syncer *replication.BinlogSyncer,
	streamer *replication.BinlogStreamer,
	currentFile *string,
	lastPos *uint32,
	lastGTID *string,
	qTimeout time.Duration,
	esTimeout time.Duration,
) error {
	var lastEventTS uint32
	var pending []int64
	// flush config
	intervalMs := task.Bulk.FlushIntervalMs
	if intervalMs <= 0 {
		intervalMs = 500
	}
	threshold := task.Bulk.Size
	if threshold <= 0 {
		threshold = 100
	}
	intervalDur := time.Duration(intervalMs) * time.Millisecond
	maxPending := r.cfg.Realtime.MaxPending
	if maxPending <= 0 {
		maxPending = 10000
	}
	r.log.Info("realtime flush config", zap.Int("threshold", threshold), zap.Int("flushIntervalMs", intervalMs))
	var lastFlushAt time.Time
	var firstPendingAt time.Time
	var lastPosSave time.Time

	for {
		// exit handling
		if err := ctx.Err(); err != nil {
			if len(pending) > 0 {
				_ = r.flush(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, &lastEventTS, currentFile, lastPos, lastGTID, pending)
			}
			return err
		}
		// time-window flush before pulling new events
		if len(pending) > 0 {
			base := lastFlushAt
			if !firstPendingAt.IsZero() {
				base = firstPendingAt
			}
			if time.Since(base) >= intervalDur {
				if lastEventTS != 0 {
					lag := time.Since(time.Unix(int64(lastEventTS), 0))
					r.log.Debug("time-window flush", zap.Int("pending", len(pending)), zap.Duration("lag", lag))
				} else {
					r.log.Debug("time-window flush", zap.Int("pending", len(pending)))
				}
				if err := r.flush(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, &lastEventTS, currentFile, lastPos, lastGTID, pending); err != nil {
					r.log.Error("flush failed", zap.Error(err))
				}
				pending = pending[:0]
				lastFlushAt = time.Now()
				firstPendingAt = time.Time{}
				continue
			}
		}
		// periodic position save
		posEvery := time.Duration(r.cfg.Realtime.PositionSaveIntervalMs) * time.Millisecond
		if posEvery <= 0 {
			posEvery = 30 * time.Second
		}
		if time.Since(lastPosSave) >= posEvery {
			if r.cfg.Position.UseGTID && r.cfg.DataSource.GTID {
				var cur string
				if err := r.db.DB.Get(&cur, "SELECT @@global.gtid_executed"); err == nil {
					_ = r.posStore.Save(position.State{GTID: cur})
					lastPosSave = time.Now()
					r.log.Debug("periodic save gtid position", zap.String("gtid_executed", cur))
				}
			} else {
				if err := r.posStore.Save(position.State{File: *currentFile, Pos: *lastPos}); err == nil {
					lastPosSave = time.Now()
					r.log.Debug("periodic save file/pos", zap.String("file", *currentFile), zap.Uint32("pos", *lastPos))
				}
			}
		}
		// pull event with timeout driving time-window flush
		tctx, cancel := context.WithTimeout(ctx, intervalDur)
		ev, err := streamer.GetEvent(tctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// try time-window flush on timeout
				if len(pending) > 0 {
					base := lastFlushAt
					if !firstPendingAt.IsZero() {
						base = firstPendingAt
					}
					if time.Since(base) >= intervalDur {
						if lastEventTS != 0 {
							lag := time.Since(time.Unix(int64(lastEventTS), 0))
							r.log.Debug("time-window flush", zap.Int("pending", len(pending)), zap.Duration("lag", lag))
						} else {
							r.log.Debug("time-window flush", zap.Int("pending", len(pending)))
						}
						if err := r.flush(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, &lastEventTS, currentFile, lastPos, lastGTID, pending); err != nil {
							r.log.Error("flush failed", zap.Error(err))
						}
						pending = pending[:0]
						lastFlushAt = time.Now()
						firstPendingAt = time.Time{}
					}
				}
				continue
			}
			// reconnect
			r.log.Warn("GetEvent error, reconnecting", zap.Error(err))
			metrics.ReconnectTotal.Inc()
			syncer.Close()
			syncer = replication.NewBinlogSyncer(syncCfg)
			st2, _ := r.posStore.Load()
			if r.cfg.Position.UseGTID && r.cfg.DataSource.GTID {
				gtid := st2.GTID
				if gtid == "" {
					if e := r.db.DB.Get(&gtid, "SELECT @@global.gtid_executed"); e != nil {
						return err
					}
				}
				if set, e := mysqlpkg.ParseGTIDSet(r.cfg.DataSource.Flavor, gtid); e == nil {
					if streamer, e = syncer.StartSyncGTID(set); e != nil {
						return e
					}
					continue
				}
				return err
			} else {
				file := st2.File
				pos := st2.Pos
				if file == "" || pos == 0 {
					file = *currentFile
					pos = *lastPos
				}
				streamer, err = syncer.StartSync(mysqlpkg.Position{Name: file, Pos: pos})
				if err != nil {
					return err
				}
				*currentFile, *lastPos = file, pos
				continue
			}
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			*currentFile = string(e.NextLogName)
			*lastPos = uint32(e.Position)
		case *replication.RowsEvent:
			schema := string(e.Table.Schema)
			physicalTable := strings.ToLower(string(e.Table.Table))
			// routingTable: optional rewrite physical table name -> logical table name (config-driven)
			routingTable := rewriteTableName(physicalTable, task.TableRewrite)
			lastEventTS = ev.Header.Timestamp
			// 仅处理/记录与任务相关的表
			if _, ok := task.MappingTable[routingTable]; !ok {
				if _, ok2 := task.RelatedQuery[routingTable]; !ok2 {
					break
				}
			}
			// 二级调试：仅对相关表记录原始 binlog 行事件信息，便于观察 MySQL 端发生了什么
			r.log.Debug("binlog行事件",
				zap.String("schema", schema),
				zap.String("table", routingTable),
				zap.String("physical_table", physicalTable),
				zap.String("type", ev.Header.EventType.String()),
				zap.Int("rowsets", len(e.Rows)),
				zap.Uint32("logpos", ev.Header.LogPos),
			)
			// schema cache must use physical table name
			cols, err := schemaCache.get(r.db.DB, schema, physicalTable)
			if err != nil {
				r.log.Warn("schema cache get failed", zap.Error(err))
				break
			}
			// 主键列名（仅主表直连路径需要；子表可通过 relatedQuery 回溯，不强制）
			keyName := ""
			if arr, ok := task.MappingTable[routingTable]; ok && len(arr) > 0 {
				keyName = arr[0]
			}
			keyIdx := -1
			if keyName != "" {
				// 找列索引
				for i, n := range cols {
					if strings.EqualFold(n, keyName) {
						keyIdx = i
						break
					}
				}
				if keyIdx < 0 {
					r.log.Warn("key column not found", zap.String("table", routingTable))
					break
				}
			}
			// 拿行
			var rows [][]interface{}
			delIDs := make([]int64, 0)
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
				replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
				replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				rows = e.Rows
			default:
				rows = e.Rows
			}
			// UPDATE diff 打印（对大字段做降噪）
			if r.cfg.Debug.SQL && (ev.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2) {
				if len(e.Rows)%2 == 0 {
					for i := 0; i < len(e.Rows); i += 2 {
						beforeRow := e.Rows[i]
						afterRow := e.Rows[i+1]
						var key interface{}
						if keyIdx >= 0 && keyIdx < len(afterRow) {
							key = afterRow[keyIdx]
						}
						changed := make(map[string][2]interface{})
						norm := func(v interface{}) interface{} {
							switch t := v.(type) {
							case []byte:
								return string(t)
							default:
								return v
							}
						}
						// 需要降噪的大字段（仅标记变化，不打印全文）
						suppress := map[string]bool{"description": true, "item_specifics": true}
						for ci := 0; ci < len(cols) && ci < len(beforeRow) && ci < len(afterRow); ci++ {
							col := strings.ToLower(cols[ci])
							bv := norm(beforeRow[ci])
							av := norm(afterRow[ci])
							if bv != av {
								if suppress[col] {
									changed[col] = [2]interface{}{"<changed large text>", "<changed large text>"}
									continue
								}
								const maxLen = 80
								trim := func(x interface{}) interface{} {
									if s, ok := x.(string); ok {
										if len(s) > maxLen {
											return s[:maxLen] + "..."
										}
										return s
									}
									return x
								}
								changed[col] = [2]interface{}{trim(bv), trim(av)}
							}
						}
						if len(changed) > 0 {
							r.log.Debug("update diff", zap.String("table", routingTable), zap.Any("key", key), zap.Any("changed", changed))
						} else {
							r.log.Debug("update diff", zap.String("table", routingTable), zap.Any("key", key), zap.String("changed", "<none>"))
						}
					}
				}
			}
			var affected []int64
			// Optional override main table name for routing (useful when mapping.sql contains template variables)
			effectiveMainTable := mainTable
			if strings.TrimSpace(task.Mapping.MainTable) != "" {
				effectiveMainTable = strings.ToLower(strings.TrimSpace(task.Mapping.MainTable))
			}
			if strings.EqualFold(routingTable, effectiveMainTable) {
				if keyName == "" {
					r.log.Warn("no key defined for table", zap.String("table", routingTable))
					break
				}
				for _, rrow := range rows {
					if keyIdx >= len(rrow) {
						continue
					}
					v := rrow[keyIdx]
					var id int64
					switch t := v.(type) {
					case int8, int16, int32, int64:
						id = reflect.ValueOf(t).Int()
					case uint8, uint16, uint32, uint64:
						id = int64(reflect.ValueOf(t).Uint())
					case []byte:
						var tmp int64
						_, _ = fmt.Sscanf(string(t), "%d", &tmp)
						id = tmp
					}
					if id != 0 {
						// 主表删除事件，且开启了 DeleteOnDelete，则直接加入删除集合并跳过重算
						if (ev.Header.EventType == replication.DELETE_ROWS_EVENTv1 || ev.Header.EventType == replication.DELETE_ROWS_EVENTv2) && task.Mapping.DeleteOnDelete {
							delIDs = append(delIDs, id)
							continue
						}
						affected = append(affected, id)
					}
				}
			} else if rule, ok := task.RelatedQuery[routingTable]; ok {
				for _, rrow := range rows {
					rowMap := make(map[string]interface{}, len(cols))
					for i, col := range cols {
						if i < len(rrow) {
							rowMap[strings.ToLower(col)] = rrow[i]
						}
					}
					bind := map[string]interface{}{}
					for param, col := range rule.Bind {
						if val, ok2 := rowMap[strings.ToLower(col)]; ok2 {
							bind[param] = val
						}
					}
					if r.cfg.Debug.SQL && r.cfg.Debug.SQLParams {
						r.log.Debug("relatedQuery bind.sample", zap.String("table", routingTable), zap.Any("bind", bind))
						// 解析路径标记：通过 SQL 中的 COALESCE(:param, ...) 动态推断直连分支参数
						// 若该参数在本次 bind 中存在且为非零/非空，则判定为 direct:param，否则为 join_fallback
						re := regexp.MustCompile(`(?i)coalesce\s*\(\s*:(\w+)`)
						if m := re.FindStringSubmatch(rule.SQL); len(m) == 2 {
							p := m[1]
							path := "join_fallback"
							if v, ok := bind[p]; ok {
								valid := false
								switch t := v.(type) {
								case int, int8, int16, int32, int64:
									valid = reflect.ValueOf(t).Int() != 0
								case uint, uint8, uint16, uint32, uint64:
									valid = reflect.ValueOf(t).Uint() != 0
								case []byte:
									valid = len(t) > 0 && string(t) != "0"
								case string:
									valid = t != "" && t != "0"
								default:
									valid = v != nil
								}
								if valid {
									path = "direct:" + p
								}
							}
							r.log.Debug("relatedQuery path", zap.String("table", routingTable), zap.String("path", path))
						}
					}

					// 判断是否需要游标分页
					useCursorPaging := rule.OrderKey != "" && rule.PageSize > 0
					if useCursorPaging {
						// 游标分页模式：循环查询直到没有更多数据
						cursor := int64(0)
						pageNum := 0
						maxPages := r.cfg.Realtime.RelatedQueryMaxPages
						if maxPages <= 0 {
							maxPages = 1000
						}

						for {
							pageNum++
							if pageNum > maxPages {
								r.log.Warn("relatedQuery 达到最大分页数限制", zap.String("table", routingTable), zap.Int("maxPages", maxPages))
								break
							}

							// 添加游标参数
							bind["cursor"] = cursor
							bind["page_size"] = rule.PageSize

							// 构建完整SQL：原SQL + ORDER BY + LIMIT
							fullSQL := rule.SQL
							if !strings.Contains(strings.ToUpper(fullSQL), "ORDER BY") {
								fullSQL = fullSQL + fmt.Sprintf(" ORDER BY %s ASC", rule.OrderKey)
							}
							if !strings.Contains(strings.ToUpper(fullSQL), "LIMIT") {
								fullSQL = fullSQL + " LIMIT :page_size"
							}

							namedSQL, args, nerr := sqlx.Named(fullSQL, bind)
							if nerr != nil {
								r.log.Warn("relatedQuery named bind failed", zap.String("table", routingTable), zap.Error(nerr))
								break
							}

							var pageIDs []int64
							q := r.db.DB.Rebind(namedSQL)
							if err := r.db.DB.Select(&pageIDs, q, args...); err != nil {
								r.log.Warn("relatedQuery select failed", zap.String("table", routingTable), zap.Int("page", pageNum), zap.Error(err))
								break
							}

							if len(pageIDs) == 0 {
								// 没有更多数据
								break
							}

							affected = append(affected, pageIDs...)

							// 更新游标为本页最后一个ID
							cursor = pageIDs[len(pageIDs)-1]

							if r.cfg.Debug.SQL {
								r.log.Debug("relatedQuery 游标分页",
									zap.String("table", routingTable),
									zap.Int("page", pageNum),
									zap.Int("count", len(pageIDs)),
									zap.Int64("cursor", cursor))
							}

							// 如果返回数据少于pageSize，说明已经是最后一页
							if len(pageIDs) < rule.PageSize {
								break
							}
						}
					} else {
						// 非分页模式：单次查询
						namedSQL, args, nerr := sqlx.Named(rule.SQL, bind)
						if nerr == nil {
							var ids []int64
							q := r.db.DB.Rebind(namedSQL)
							if err := r.db.DB.Select(&ids, q, args...); err != nil {
								r.log.Warn("relatedQuery select failed", zap.String("table", routingTable), zap.Error(err))
							} else {
								affected = append(affected, ids...)
							}
						} else {
							r.log.Warn("relatedQuery named bind failed", zap.String("table", routingTable), zap.Error(nerr))
						}
					}
				}
			}
			// 子表删除事件触发重算的可视化
			if (ev.Header.EventType == replication.DELETE_ROWS_EVENTv1 || ev.Header.EventType == replication.DELETE_ROWS_EVENTv2) && !strings.EqualFold(routingTable, effectiveMainTable) && len(affected) > 0 {
				ids := make([]string, 0, len(affected))
				for _, id := range affected {
					ids = append(ids, fmt.Sprintf("%d", id))
				}
				sample := ids
				if len(sample) > 5 {
					sample = sample[:5]
				}
				r.log.Debug("子表删除事件, 触发重算", zap.String("table", routingTable), zap.String("index", index), zap.Int("ids", len(ids)), zap.Any("ids.sample", sample))
			}

			// 追加 pending
			for _, id := range affected {
				if id == 0 {
					continue
				}
				if len(pending) == 0 {
					firstPendingAt = time.Now()
					r.log.Debug("pending first append", zap.Int64("id", id))
				}
				pending = append(pending, id)
				if len(pending) >= maxPending {
					r.log.Warn("pending overflow, force flush", zap.Int("pending", len(pending)), zap.Int("maxPending", maxPending))
					if err := r.flush(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, &lastEventTS, currentFile, lastPos, lastGTID, pending); err != nil {
						r.log.Error("flush failed", zap.Error(err))
					}
					pending = pending[:0]
					lastFlushAt = time.Now()
					firstPendingAt = time.Time{}
					continue
				}
			}
			// 若存在主表删除事件，按需立即删除（不走重算）
			if len(delIDs) > 0 {
				idStr := make([]string, 0, len(delIDs))
				for _, id := range delIDs {
					idStr = append(idStr, fmt.Sprintf("%d", id))
				}
				sample := idStr
				if len(sample) > 5 {
					sample = sample[:5]
				}
				r.log.Debug("主表删除事件, 立即删除(DeleteOnDelete)", zap.String("index", index), zap.Any("ids", idStr), zap.Any("ids.sample", sample))
				if _, err := bw.Delete(ctx, esTimeout, index, idStr); err != nil {
					r.log.Error("es bulk delete failed(DeleteOnDelete)", zap.Error(err), zap.Int("ids", len(idStr)))
				} else if lastEventTS != 0 {
					lag := time.Since(time.Unix(int64(lastEventTS), 0))
					metrics.RealtimeBinlogLagSeconds.Set(lag.Seconds())
				}
			}
			// 批量阈值触发
			if len(pending) >= threshold {
				if err := r.flush(ctx, task, index, mainSQL, bw, qTimeout, esTimeout, &lastEventTS, currentFile, lastPos, lastGTID, pending); err != nil {
					r.log.Error("flush failed", zap.Error(err))
				}
				pending = pending[:0]
				lastFlushAt = time.Now()
				firstPendingAt = time.Time{}
			}
		case *replication.XIDEvent:
			*lastPos = ev.Header.LogPos
		default:
			// ignore
		}
	}
}
