package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Report is a persistable snapshot of one load run. It is tagged with enough
// provenance (commit SHA, server version, params) that a previously-saved run
// can be reused as a comparison baseline without re-running it.
type Report struct {
	Scenario      string            `json:"scenario"`
	Addr          string            `json:"addr"`
	Ref           string            `json:"ref,omitempty"`            // git ref requested (e.g. "HEAD", "baseline-perf")
	CommitSHA     string            `json:"commit_sha,omitempty"`     // resolved commit, the real identity of the version
	ServerVersion string            `json:"server_version,omitempty"` // from the TASSERVER greeting
	Params        map[string]string `json:"params,omitempty"`         // scenario knobs (conns, ramp, duration, ...)
	StartedAt     string            `json:"started_at,omitempty"`     // RFC3339
	DurationSec   float64           `json:"duration_sec"`
	Commands      []CmdStat         `json:"commands"`
	Counters      map[string]int64  `json:"counters"`
}

// CmdStat is the latency summary for one command type.
type CmdStat struct {
	Command   string  `json:"command"`
	Count     int     `json:"count"`
	P50ms     float64 `json:"p50_ms"`
	P95ms     float64 `json:"p95_ms"`
	P99ms     float64 `json:"p99_ms"`
	MaxMs     float64 `json:"max_ms"`
	PerSecond float64 `json:"per_second"`
}

// BuildReport assembles a Report from the Recorder's accumulated data.
func (r *Recorder) BuildReport(scenario, addr string, dur time.Duration) Report {
	r.mu.Lock()
	defer r.mu.Unlock()

	rep := Report{
		Scenario:    scenario,
		Addr:        addr,
		DurationSec: dur.Seconds(),
		Counters:    make(map[string]int64, len(r.counters)),
	}
	for k, v := range r.counters {
		rep.Counters[k] = v
	}
	names := make([]string, 0, len(r.hists))
	for name := range r.hists {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		count, p50, p95, p99, max := r.hists[name].summary()
		perSec := 0.0
		if dur > 0 {
			perSec = float64(count) / dur.Seconds()
		}
		rep.Commands = append(rep.Commands, CmdStat{
			Command:   name,
			Count:     count,
			P50ms:     ms(p50),
			P95ms:     ms(p95),
			P99ms:     ms(p99),
			MaxMs:     ms(max),
			PerSecond: perSec,
		})
	}
	return rep
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// WriteJSON serialises the report as indented JSON.
func (rep Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

// WriteHuman prints a compact human-readable table.
func (rep Report) WriteHuman(w io.Writer) {
	fmt.Fprintf(w, "scenario=%s addr=%s ref=%s sha=%s dur=%.1fs\n",
		rep.Scenario, rep.Addr, dash(rep.Ref), shortSHA(rep.CommitSHA), rep.DurationSec)
	fmt.Fprintf(w, "%-16s %8s %9s %9s %9s %9s %9s\n", "command", "count", "p50ms", "p95ms", "p99ms", "maxms", "per_s")
	for _, c := range rep.Commands {
		fmt.Fprintf(w, "%-16s %8d %9.2f %9.2f %9.2f %9.2f %9.1f\n",
			c.Command, c.Count, c.P50ms, c.P95ms, c.P99ms, c.MaxMs, c.PerSecond)
	}
	if len(rep.Counters) > 0 {
		keys := make([]string, 0, len(rep.Counters))
		for k := range rep.Counters {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(w, "counters: ")
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s=%d", k, rep.Counters[k]))
		}
		fmt.Fprintln(w, strings.Join(parts, " "))
	}
}

// Save writes the report to dir as <scenario>__<ref-or-sha>__<started>.json and
// returns the path. The filename encodes provenance so a results directory is
// self-describing and reusable across days.
func (rep Report) Save(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tag := rep.Ref
	if rep.CommitSHA != "" {
		tag = shortSHA(rep.CommitSHA)
	}
	if tag == "" {
		tag = "adhoc"
	}
	stamp := strings.ReplaceAll(rep.StartedAt, ":", "-")
	if stamp == "" {
		stamp = "unstamped"
	}
	name := fmt.Sprintf("%s__%s__%s.json", safe(rep.Scenario), safe(tag), safe(stamp))
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := rep.WriteJSON(f); err != nil {
		return "", err
	}
	return path, nil
}

// LoadReport reads a saved report from a JSON file.
func LoadReport(path string) (Report, error) {
	var rep Report
	b, err := os.ReadFile(path)
	if err != nil {
		return rep, err
	}
	err = json.Unmarshal(b, &rep)
	return rep, err
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func shortSHA(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	if s == "" {
		return "-"
	}
	return s
}

func safe(s string) string {
	r := strings.NewReplacer("/", "-", " ", "_", string(filepath.Separator), "-")
	return r.Replace(s)
}
