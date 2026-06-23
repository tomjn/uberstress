package workload

import (
	"context"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
)

// Execute runs a scenario with the given number of background reactor-health
// pingers, into rec. The pingers run for the lifetime of the scenario and are
// stopped once it returns. Shared by the `load` and `bench` commands.
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

	err := sc.Run(ctx, cfg, rec)

	cancel()
	wg.Wait()
	return err
}
