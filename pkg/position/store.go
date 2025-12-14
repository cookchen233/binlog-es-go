package position

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// State represents a replication position, supporting GTID or file/pos.
type State struct {
	// GTID for GTID-based replication. Empty if using file/pos.
	GTID string `json:"gtid,omitempty"`
	// File/Pos for classic replication position.
	File string `json:"file,omitempty"`
	Pos  uint32 `json:"pos,omitempty"`

	// Meta
	UpdatedAt time.Time `json:"updated_at"`
}

// Store persists State to a JSON file.
type Store struct {
	path string
	mu   sync.RWMutex
}

func New(path string) *Store { return &Store{path: path} }

// Load reads the position file. If not exists, returns empty State and nil error.
func (s *Store) Load() (State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var st State
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return st, err
	}
	if len(b) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, err
	}
	return st, nil
}

// Save writes the position file atomically.
func (s *Store) Save(st State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st.UpdatedAt = time.Now()
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), fs.FileMode(0o755)); err != nil {
		return err
	}
	// write temp then rename for atomicity
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
