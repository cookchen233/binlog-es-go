package util

import (
	"time"
)

// Retry runs fn with retry and backoff. If backoffMs is empty, uses 200ms,400ms,800ms.
// It returns nil on first success, or the last error after exhausting attempts.
func Retry(attempts int, backoffMs []int, fn func() error, sleep func(time.Duration)) error {
	if attempts <= 0 {
		attempts = 1
	}
	if len(backoffMs) == 0 {
		backoffMs = []int{200, 400, 800, 1600, 3200}
	}
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if i < attempts-1 {
			d := time.Duration(backoffMs[min(i, len(backoffMs)-1)]) * time.Millisecond
			if sleep != nil {
				sleep(d)
			} else {
				time.Sleep(d)
			}
		}
	}
	return err
}

func min(a, b int) int { if a < b { return a }; return b }
