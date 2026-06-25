package workload

import (
	"context"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
)

func init() { register(social{}) }

// social logs in Conns seeded clients and has each cycle through the async
// social commands at a paced rate: IGNORE then UNIGNORE a peer (the async
// insert/delete write path) and IGNORELIST then FRIENDLIST (the async list-read
// path). Each command defers its DB work off the reactor on the new build, so
// the background pinger should stay flat where the old synchronous build would
// stall. Latency is recorded per command (IGNORE/UNIGNORE/IGNORELIST/FRIENDLIST).
//
// IGNORE needs a real, non-self target; client i ignores peer (i+1)%Conns, which
// the seed phase has already created. The strict IGNORE->UNIGNORE ordering each
// cycle keeps both writes hitting a real INSERT/DELETE rather than a no-op.
type social struct{}

func (social) Name() string { return "social" }
func (social) Describe() string {
	return "paced IGNORE/UNIGNORE + IGNORELIST/FRIENDLIST per client; targets async social read/write"
}

func (social) Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error {
	if cfg.Register {
		seedAccounts(ctx, cfg, rec)
		sleepCtx(ctx, 500*time.Millisecond)
	}

	loadCtx, cancel := context.WithTimeout(ctx, cfg.Ramp+cfg.Duration)
	defer cancel()

	opInterval := cfg.SayInterval
	if opInterval <= 0 {
		opInterval = time.Second
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
			runOneSocial(loadCtx, cfg, id, opInterval, rec)
		}(i)
	}
	wg.Wait()
	return nil
}

func runOneSocial(ctx context.Context, cfg Config, id int, opInterval time.Duration, rec *metrics.Recorder) {
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

	// Peer to ignore/unignore; with Conns>=2 this is always another seeded user.
	peer := accountName(cfg.UserPrefix, (id+1)%cfg.Conns)

	const timeout = 10 * time.Second
	seq := 0
	t := time.NewTicker(opInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			var (
				key, ctr string
				d        time.Duration
				err      error
			)
			switch seq % 4 {
			case 0:
				key, ctr = "IGNORE", "ignore"
				d, err = c.Ignore(peer, timeout)
			case 1:
				key, ctr = "UNIGNORE", "unignore"
				d, err = c.Unignore(peer, timeout)
			case 2:
				key, ctr = "IGNORELIST", "ignorelist"
				d, err = c.IgnoreList(timeout)
			default:
				key, ctr = "FRIENDLIST", "friendlist"
				d, err = c.FriendList(timeout)
			}
			if err != nil {
				rec.ObserveError(key)
				rec.Inc(ctr + "_error")
				return
			}
			rec.Observe(key, d)
			rec.Inc(ctr + "_ok")
			seq++
		}
	}
}
