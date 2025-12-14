package es

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
)

// BulkError represents a structured bulk operation error that
// includes conflicted IDs for specialized handling upstream.
type BulkError struct {
	// ConflictedIDs carries IDs that failed with version_conflict_engine_exception.
	ConflictedIDs []string
	// Reasons aggregates human-readable error reasons for logging.
	Reasons []string
}

// Error implements the error interface.
func (e *BulkError) Error() string {
	if len(e.Reasons) == 0 {
		return "bulk has errors"
	}
	return fmt.Sprintf("bulk has errors: %s", strings.Join(e.Reasons, "; "))
}

// Writer wraps an elastic client and write options.
type Writer struct {
	client *elastic.Client
	// optional refresh policy: "", "false", "wait_for", "true" (true behaves like wait_for in newer ES)
	refresh string
}

// DeleteBulk deletes documents by IDs from the specified index.
// IDs must be stringifiable. Returns the number of items attempted (length of bulk items) and error if any.
func (w *Writer) DeleteBulk(ctx context.Context, index string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	bulk := w.client.Bulk().Index(index)
	if w.refresh != "" {
		bulk = bulk.Refresh(w.refresh)
	}
	for _, id := range ids {
		if id == "" {
			continue
		}
		req := elastic.NewBulkDeleteRequest().Index(index).Id(id)
		bulk = bulk.Add(req)
	}
	res, err := bulk.Do(ctx)
	if err != nil {
		return 0, err
	}
	if res.Errors {
		var reasons []string
		var conflicted []string
		for _, byIndex := range res.Items {
			for action, item := range byIndex {
				if item.Error != nil {
					reasons = append(reasons, fmt.Sprintf("%s id=%s type=%s reason=%s", action, item.Id, item.Error.Type, item.Error.Reason))
					if strings.EqualFold(item.Error.Type, "version_conflict_engine_exception") {
						conflicted = append(conflicted, item.Id)
					}
				}
			}
		}
		if len(reasons) > 0 {
			return 0, &BulkError{ConflictedIDs: conflicted, Reasons: reasons}
		}
	}
	return len(res.Items), nil
}

// New creates an ES v7 client.
func New(addresses []string, username, password string) (*Writer, error) {
	opts := []elastic.ClientOptionFunc{
		elastic.SetURL(addresses...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(true),
		elastic.SetHealthcheckInterval(10 * time.Second),
	}
	if username != "" {
		opts = append(opts, elastic.SetBasicAuth(username, password))
	}
	cli, err := elastic.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Writer{client: cli}, nil
}

// UpsertBulk upserts documents where each doc must contain the id field specified by idField.
// Docs should be plain maps (string->primitive or nested maps/arrays).
func (w *Writer) UpsertBulk(ctx context.Context, index string, idField string, docs []map[string]interface{}) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	bulk := w.client.Bulk().Index(index)
	if w.refresh != "" {
		bulk = bulk.Refresh(w.refresh)
	}
	for _, d := range docs {
		idVal, ok := d[idField]
		if !ok {
			return 0, fmt.Errorf("doc missing id field: %s", idField)
		}
		id := fmt.Sprintf("%v", idVal)
		// remove id field from source body to avoid mapper_parsing_exception if it's not part of source
		payload := make(map[string]interface{}, len(d))
		for k, v := range d {
			if k == idField {
				continue
			}
			payload[k] = v
		}
		// use BulkUpdateRequest with DocAsUpsert
		req := elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(payload).DocAsUpsert(true)
		bulk = bulk.Add(req)
	}
	res, err := bulk.Do(ctx)
	if err != nil {
		return 0, err
	}
	// check per-item errors
	if res.Errors {
		var reasons []string
		var conflicted []string
		for _, byIndex := range res.Items {
			for action, item := range byIndex {
				if item.Error != nil {
					reasons = append(reasons, fmt.Sprintf("%s id=%s type=%s reason=%s", action, item.Id, item.Error.Type, item.Error.Reason))
					if strings.EqualFold(item.Error.Type, "version_conflict_engine_exception") {
						conflicted = append(conflicted, item.Id)
					}
				}
			}
		}
		if len(reasons) > 0 {
			return 0, &BulkError{ConflictedIDs: conflicted, Reasons: reasons}
		}
	}
	return len(res.Items), nil
}

// SetRefresh sets the bulk refresh policy ("", "false", "wait_for", or "true").
func (w *Writer) SetRefresh(refresh string) {
	w.refresh = refresh
}

// Ping checks cluster health to validate connectivity.
func (w *Writer) Ping(ctx context.Context) error {
	// ClusterHealth is a cheap call to validate connectivity
	_, err := w.client.ClusterHealth().Do(ctx)
	return err
}

// IndexExists checks if an index exists.
func (w *Writer) IndexExists(ctx context.Context, index string) (bool, error) {
	return w.client.IndexExists(index).Do(ctx)
}
