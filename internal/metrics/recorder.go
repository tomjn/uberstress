// Package metrics records per-command latency distributions and named counters
// during a load run, and renders them as a persistable Report.
package metrics

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Recorder aggregates observations across all connection goroutines. Safe for
// concurrent use.
type Recorder struct {
	mu        sync.Mutex
	hists     map[string]*hist
	counters  map[string]int64
	cmdErrors map[string]int64 // per-command failure counts, keyed by command name
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		hists:     make(map[string]*hist),
		counters:  make(map[string]int64),
		cmdErrors: make(map[string]int64),
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

// ObserveError records a failed attempt of a named command (e.g. "LOGIN"),
// attributing it to that command's CmdStat. Connection-level failures that
// precede any command (dial/seed/TLS) are recorded with Inc instead, as they
// belong to no single command.
func (r *Recorder) ObserveError(command string) {
	r.mu.Lock()
	r.cmdErrors[command]++
	r.mu.Unlock()
}

// Snapshot returns a cheap mid-run summary for live progress reporting: the
// total number of recorded command observations and the total number of
// error-type counters seen so far.
func (r *Recorder) Snapshot() (commands, errors int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.hists {
		commands += int64(len(h.samples))
	}
	for name, v := range r.counters {
		if isErrorCounter(name) {
			errors += v
		}
	}
	return commands, errors
}

// isErrorCounter reports whether a counter name denotes a failure (mirrors the
// coilbox UI's classification). Retries are deliberately excluded.
func isErrorCounter(name string) bool {
	return strings.Contains(name, "error") ||
		strings.Contains(name, "timeout") ||
		strings.Contains(name, "fail")
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
