package es

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
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
	client *http.Client
	base   *url.URL
	user   string
	pass   string
	// optional refresh policy: "", "false", "wait_for", "true" (true behaves like wait_for in newer ES)
	refresh string
}

// TLSConfig configures HTTPS behavior.
// CAFile: optional PEM CA bundle; CertFile/KeyFile: optional client cert auth.
// InsecureSkipVerify is discouraged but sometimes needed for dev self-signed.
type TLSConfig struct {
	InsecureSkipVerify bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
}

// DeleteBulk deletes documents by IDs from the specified index.
// IDs must be stringifiable. Returns the number of items attempted (length of bulk items) and error if any.
func (w *Writer) DeleteBulk(ctx context.Context, index string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	ndjson := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		meta := fmt.Sprintf(`{"delete":{"_index":%q,"_id":%q}}`, index, id)
		ndjson = append(ndjson, meta)
	}
	if len(ndjson) == 0 {
		return 0, nil
	}

	res, err := w.doBulk(ctx, strings.Join(ndjson, "\n")+"\n")
	if err != nil {
		return 0, err
	}
	if res.Errors {
		return 0, res.toBulkError()
	}
	return len(res.Items), nil
}

// New creates a lightweight HTTP ES client compatible with Elasticsearch 7/8/9.
func New(addresses []string, username, password string) (*Writer, error) {
	return NewWithTLS(addresses, username, password, TLSConfig{})
}

// NewWithTLS creates a lightweight HTTP ES client compatible with Elasticsearch 7/8/9,
// with optional TLS config for https addresses.
func NewWithTLS(addresses []string, username, password string, tlsCfg TLSConfig) (*Writer, error) {
	if len(addresses) == 0 {
		return nil, fmt.Errorf("es addresses empty")
	}
	base, err := url.Parse(addresses[0])
	if err != nil {
		return nil, err
	}
	if base.Scheme == "" {
		return nil, fmt.Errorf("invalid es address (missing scheme): %s", addresses[0])
	}
	if base.Host == "" {
		return nil, fmt.Errorf("invalid es address (missing host): %s", addresses[0])
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.EqualFold(base.Scheme, "https") {
		tlsConf, err := buildTLSConfig(tlsCfg)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsConf
	}
	return &Writer{
		client: &http.Client{Timeout: 30 * time.Second, Transport: transport},
		base:   base,
		user:   username,
		pass:   password,
	}, nil
}

func buildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tlsConf := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	if cfg.ServerName != "" {
		tlsConf.ServerName = cfg.ServerName
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read es tls caFile: %w", err)
		}
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("parse es tls caFile: no valid PEM certs")
		}
		tlsConf.RootCAs = pool
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, fmt.Errorf("es tls requires both certFile and keyFile")
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load es tls cert/key: %w", err)
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}
	return tlsConf, nil
}

// UpsertBulk upserts documents where each doc must contain the id field specified by idField.
// Docs should be plain maps (string->primitive or nested maps/arrays).
func (w *Writer) UpsertBulk(ctx context.Context, index string, idField string, docs []map[string]interface{}) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	ndjson := make([]string, 0, len(docs)*2)
	for _, d := range docs {
		idVal, ok := d[idField]
		if !ok {
			return 0, fmt.Errorf("doc missing id field: %s", idField)
		}
		id := fmt.Sprintf("%v", idVal)
		payload := make(map[string]interface{}, len(d))
		for k, v := range d {
			if k == idField {
				continue
			}
			payload[k] = v
		}
		meta := fmt.Sprintf(`{"update":{"_index":%q,"_id":%q}}`, index, id)
		src, err := json.Marshal(map[string]interface{}{
			"doc":           payload,
			"doc_as_upsert": true,
		})
		if err != nil {
			return 0, err
		}
		ndjson = append(ndjson, meta, string(src))
	}

	res, err := w.doBulk(ctx, strings.Join(ndjson, "\n")+"\n")
	if err != nil {
		return 0, err
	}
	if res.Errors {
		return 0, res.toBulkError()
	}
	return len(res.Items), nil
}

// SetRefresh sets the bulk refresh policy ("", "false", "wait_for", or "true").
func (w *Writer) SetRefresh(refresh string) {
	w.refresh = refresh
}

// Ping checks cluster health to validate connectivity.
func (w *Writer) Ping(ctx context.Context) error {
	req, err := w.newRequest(ctx, http.MethodGet, "_cluster/health", nil, nil)
	if err != nil {
		return err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("es ping failed: status=%d body=%s", resp.StatusCode, string(body))
}

// IndexExists checks if an index exists.
func (w *Writer) IndexExists(ctx context.Context, index string) (bool, error) {
	req, err := w.newRequest(ctx, http.MethodHead, index, nil, nil)
	if err != nil {
		return false, err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return false, fmt.Errorf("es index exists check failed: status=%d body=%s", resp.StatusCode, string(body))
}

type bulkResponse struct {
	Errors bool                     `json:"errors"`
	Items  []map[string]bulkItemRes `json:"items"`
}

type bulkItemRes struct {
	ID     string        `json:"_id"`
	Status int           `json:"status"`
	Error  *bulkItemErr  `json:"error"`
}

type bulkItemErr struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (r *bulkResponse) toBulkError() error {
	var reasons []string
	var conflicted []string
	for _, byAction := range r.Items {
		for action, item := range byAction {
			if item.Error == nil && item.Status != http.StatusConflict {
				continue
			}
			errType := ""
			errReason := ""
			if item.Error != nil {
				errType = item.Error.Type
				errReason = item.Error.Reason
			}
			reasons = append(reasons, fmt.Sprintf("%s id=%s status=%d type=%s reason=%s", action, item.ID, item.Status, errType, errReason))
			if strings.EqualFold(errType, "version_conflict_engine_exception") || item.Status == http.StatusConflict {
				conflicted = append(conflicted, item.ID)
			}
		}
	}
	if len(reasons) == 0 {
		reasons = []string{"bulk has errors"}
	}
	return &BulkError{ConflictedIDs: conflicted, Reasons: reasons}
}

func (w *Writer) doBulk(ctx context.Context, ndjson string) (*bulkResponse, error) {
	q := url.Values{}
	if w.refresh != "" {
		q.Set("refresh", w.refresh)
	}
	req, err := w.newRequest(ctx, http.MethodPost, "_bulk", strings.NewReader(ndjson), q)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// bulk API usually returns 200 even with errors, but keep a guard
		if len(body) > 4096 {
			body = body[:4096]
		}
		return nil, fmt.Errorf("es bulk http failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	var br bulkResponse
	if err := json.Unmarshal(body, &br); err != nil {
		if len(body) > 4096 {
			body = body[:4096]
		}
		return nil, fmt.Errorf("es bulk decode failed: %w body=%s", err, string(body))
	}
	return &br, nil
}

func (w *Writer) newRequest(ctx context.Context, method, path string, body io.Reader, q url.Values) (*http.Request, error) {
	u := *w.base
	// url.JoinPath is go1.19+, but to avoid subtle behavior with leading '/', build manually.
	basePath := strings.TrimRight(u.Path, "/")
	addPath := strings.TrimLeft(path, "/")
	if basePath == "" {
		u.Path = "/" + addPath
	} else {
		u.Path = basePath + "/" + addPath
	}
	if q != nil {
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	if w.user != "" {
		req.SetBasicAuth(w.user, w.pass)
	}
	return req, nil
}
