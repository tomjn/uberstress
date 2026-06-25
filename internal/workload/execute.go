package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
)

// progressPrefix marks a machine-readable progress line on stderr. Consumers
// (coilbox) detect this prefix and parse the trailing JSON into a live panel;
// the lines are otherwise harmless noise in a human log.
const progressPrefix = "@us:progress "

// progressLine is the JSON payload emitted after progressPrefix each tick.
type progressLine struct {
	T      float64 `json:"t"`      // seconds since the scenario started
	Sent   int64   `json:"sent"`   // total successful command observations so far
	Rate   float64 `json:"rate"`   // commands/second since the previous tick
	Errors int64   `json:"errors"` // total error-type counters so far
}

// Execute runs a scenario with the given number of background reactor-health
// pingers, into rec. The pingers run for the lifetime of the scenario and are
// stopped once it returns. While the scenario runs, a background ticker emits
// progress lines to stderr (see progressPrefix). Shared by `load` and `bench`.
func Execute(ctx context.Context, sc Scenario, cfg Config, pingers int, pingInterval time.Duration, rec *metrics.Recorder) error {
	pctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < pingers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RunPinger(pctx, cfg.Addr, pingInterval, rec)
		}()
	}

	progDone := make(chan struct{})
	var progWG sync.WaitGroup
	progWG.Add(1)
	go func() {
		defer progWG.Done()
		emitProgress(rec, progDone)
	}()

	err := sc.Run(ctx, cfg, rec)

	close(progDone)
	progWG.Wait()
	cancel()
	wg.Wait()
	return err
}

// emitProgress writes one progress line per second until done is closed.
func emitProgress(rec *metrics.Recorder, done <-chan struct{}) {
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastSent int64
	lastAt := start
	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			sent, errs := rec.Snapshot()
			dt := now.Sub(lastAt).Seconds()
			var rate float64
			if dt > 0 {
				rate = float64(sent-lastSent) / dt
			}
			lastSent, lastAt = sent, now
			b, err := json.Marshal(progressLine{
				T:      now.Sub(start).Seconds(),
				Sent:   sent,
				Rate:   rate,
				Errors: errs,
			})
			if err != nil {
				continue
			}
			fmt.Fprintf(os.Stderr, "%s%s\n", progressPrefix, b)
		}
	}
}
