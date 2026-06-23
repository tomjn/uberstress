// Package workload defines load scenarios and the reactor-health pinger that
// run against a uberserver lobby endpoint.
package workload

import (
	"context"
	"sort"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
)

// Config holds the knobs shared by all scenarios.
type Config struct {
	Addr       string        // host:port of the lobby server
	Conns      int           // number of concurrent connections to drive
	Duration   time.Duration // how long to hold steady-state load
	Ramp       time.Duration // spread connection startup over this window
	Register   bool          // register each account before logging in
	UserPrefix string        // account name prefix; accounts are <prefix><id>
	Password   string        // shared password for generated accounts
	Channel    string        // channel used by chat-style scenarios
}

// Params renders the config as a flat string map for report provenance.
func (c Config) Params() map[string]string {
	return map[string]string{
		"conns":    itoa(c.Conns),
		"duration": c.Duration.String(),
		"ramp":     c.Ramp.String(),
		"register": btoa(c.Register),
		"channel":  c.Channel,
	}
}

// Scenario is one named load profile.
type Scenario interface {
	Name() string
	Describe() string
	// Run drives the load until ctx is cancelled, recording into rec.
	Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error
}

var registry = map[string]Scenario{}

func register(s Scenario) { registry[s.Name()] = s }

// Get returns a scenario by name.
func Get(name string) (Scenario, bool) {
	s, ok := registry[name]
	return s, ok
}

// Names returns the registered scenario names, sorted.
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// All returns the registered scenarios, sorted by name.
func All() []Scenario {
	out := make([]Scenario, 0, len(registry))
	for _, n := range Names() {
		out = append(out, registry[n])
	}
	return out
}
