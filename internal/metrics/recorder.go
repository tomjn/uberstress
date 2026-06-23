// Package metrics records per-command latency distributions and named counters
// during a load run, and renders them as a persistable Report.
package metrics

import (
	"sort"
	"sync"
	"time"
)

// Recorder aggregates observations across all connection goroutines. Safe for
// concurrent use.
type Recorder struct {
	mu       sync.Mutex
	hists    map[string]*hist
	counters map[string]int64
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		hists:    make(map[string]*hist),
		counters: make(map[string]int64),
	}
}

// Observe records a latency sample for a named command (e.g. "LOGIN", "PING").
func (r *Recorder) Observe(command string, d time.Duration) {
	r.mu.Lock()
	h := r.hists[command]
	if h == nil {
		h = &hist{}
		r.hists[command] = h
	}
	h.samples = append(h.samples, d)
	r.mu.Unlock()
}

// Inc increments a named counter (e.g. "login_error", "dial_error").
func (r *Recorder) Inc(name string) { r.Add(name, 1) }

// Add adds delta to a named counter.
func (r *Recorder) Add(name string, delta int64) {
	r.mu.Lock()
	r.counters[name] += delta
	r.mu.Unlock()
}

// hist holds raw latency samples for one command. Exact percentiles are
// computed at report time. Memory is O(samples); adequate for the connection
// counts this tool targets (thousands of conns, modest per-conn op rates).
type hist struct {
	samples []time.Duration
}

func (h *hist) summary() (count int, p50, p95, p99, max time.Duration) {
	count = len(h.samples)
	if count == 0 {
		return 0, 0, 0, 0, 0
	}
	s := make([]time.Duration, count)
	copy(s, h.samples)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return count, percentile(s, 0.50), percentile(s, 0.95), percentile(s, 0.99), s[count-1]
}

// percentile returns the p-quantile (0..1) of a sorted slice using
// nearest-rank.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(p * float64(n))
	if rank >= n {
		rank = n - 1
	}
	return sorted[rank]
}
