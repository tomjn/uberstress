package workload

import (
	"context"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
)

// RunPinger maintains a dedicated connection that issues PING at a fixed
// interval throughout a run, recording PING round-trip latency under the key
// "PING". PING is allowed pre-login and performs no database work, so its
// latency is a near-direct measure of reactor head-of-line blocking: it stays
// flat on the async build and spikes on the old synchronous build whenever a DB
// call is in flight. This is the headline old-vs-new discriminator.
//
// On any error the prober tears down and redials, because a read timeout can
// leave a bufio stream mid-line (see proto.Client.readLine); counters surface
// how often that happened.
func RunPinger(ctx context.Context, addr string, interval time.Duration, rec *metrics.Recorder) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		c, err := proto.Dial(addr, 5*time.Second)
		if err != nil {
			rec.Inc("ping_dial_error")
			if !sleepUnlessDone(ctx, interval) {
				return
			}
			continue
		}
		pingLoop(ctx, c, interval, rec)
		c.Close()
	}
}

// pingLoop runs PINGs on an established connection until ctx is done or an error
// forces a reconnect.
func pingLoop(ctx context.Context, c *proto.Client, interval time.Duration, rec *metrics.Recorder) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d, err := c.Ping(5 * time.Second)
			if err != nil {
				rec.ObserveError("PING")
				rec.Inc("ping_error")
				return // reconnect
			}
			rec.Observe("PING", d)
		}
	}
}

func sleepUnlessDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
