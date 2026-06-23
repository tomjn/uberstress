package workload

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
)

func init() { register(chat{}) }

// chat spreads Conns connections across Channels channels; each joins its
// channel and emits SAY at a paced rate for the duration. It exercises the
// per-channel pub/sub broadcast path and the async say-history write, recording
// SAY round-trip latency (send SAY -> receive our own SAID echo).
type chat struct{}

func (chat) Name() string { return "chat" }
func (chat) Describe() string {
	return "users join channels and SAY at a paced rate; targets pub/sub + async say-history"
}

func (chat) Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error {
	if cfg.Register {
		seedAccounts(ctx, cfg, rec)
		sleepCtx(ctx, 500*time.Millisecond)
	}

	loadCtx, cancel := context.WithTimeout(ctx, cfg.Ramp+cfg.Duration)
	defer cancel()

	sayInterval := cfg.SayInterval
	if sayInterval <= 0 {
		sayInterval = time.Second
	}

	var wg sync.WaitGroup
	interval := rampInterval(cfg)
	for i := 0; i < cfg.Conns; i++ {
		if loadCtx.Err() != nil {
			break
		}
		if interval > 0 {
			sleepCtx(loadCtx, interval)
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runOneChatter(loadCtx, cfg, id, sayInterval, rec)
		}(i)
	}
	wg.Wait()
	return nil
}

func runOneChatter(ctx context.Context, cfg Config, id int, sayInterval time.Duration, rec *metrics.Recorder) {
	user := accountName(cfg.UserPrefix, id)

	c, err := proto.Dial(cfg.Addr, 5*time.Second)
	if err != nil {
		rec.Inc("dial_error")
		return
	}
	defer c.Exit()

	if _, ok := loginWithRetry(ctx, c, user, cfg.Password, rec); !ok {
		return
	}

	channel := channelName(cfg.Channel, id, cfg.Channels)
	if _, err := c.Join(channel, 10*time.Second); err != nil {
		rec.Inc("join_error")
		return
	}
	rec.Inc("join_ok")

	seq := 0
	t := time.NewTicker(sayInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			seq++
			msg := fmt.Sprintf("u%d-m%d", id, seq)
			d, err := c.Say(channel, msg, 10*time.Second)
			if err != nil {
				rec.Inc("say_error")
				return
			}
			rec.Observe("SAY", d)
		}
	}
}
