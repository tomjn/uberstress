package workload

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
)

func init() { register(loginStorm{}) }

// loginStorm seeds Conns accounts (register + confirm agreement) and then, in a
// timed phase, ramps Conns fresh connections through LOGIN and holds them open.
// It targets the async-login path: under the old synchronous build the login DB
// work blocks the reactor, which the background pinger detects as elevated PING
// latency even though PING itself touches no database.
type loginStorm struct{}

func (loginStorm) Name() string { return "login-storm" }
func (loginStorm) Describe() string {
	return "seed accounts, then ramp N fresh logins and hold them; targets async login"
}

func (loginStorm) Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error {
	if cfg.Register {
		seedAccounts(ctx, cfg, rec)
		// Brief grace so seed logouts settle before fresh logins, avoiding
		// "already logged in" rejections.
		sleepCtx(ctx, 500*time.Millisecond)
	}

	// The timed phase ends after ramp + duration (or earlier on Ctrl-C).
	loadCtx, cancel := context.WithTimeout(ctx, cfg.Ramp+cfg.Duration)
	defer cancel()

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
			runOneLogin(loadCtx, cfg, id, rec)
		}(i)
	}
	wg.Wait()
	return nil
}

func runOneLogin(ctx context.Context, cfg Config, id int, rec *metrics.Recorder) {
	user := accountName(cfg.UserPrefix, id)

	c, err := proto.Dial(cfg.Addr, 5*time.Second)
	if err != nil {
		rec.Inc("dial_error")
		return
	}
	defer c.Exit()

	// Retry the transient "Already logged in." denial that can occur if a seed
	// connection for this account has not finished disconnecting yet. Only the
	// successful attempt's latency is recorded.
	var ttl time.Duration
	for attempt := 0; attempt < 6; attempt++ {
		ttl, err = c.Login(user, cfg.Password, 15*time.Second)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "Already logged in") {
			rec.Inc("login_retry")
			sleepCtx(ctx, 400*time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		rec.Inc("login_error")
		return
	}
	rec.Observe("LOGIN", ttl)
	rec.Inc("login_ok")

	// Hold the connection open so the server carries the full connected-user
	// load for the rest of the timed phase.
	<-ctx.Done()
}

// seedAccounts ensures accounts 0..Conns-1 exist and have confirmed the
// agreement, using bounded concurrency so the per-account 2s ToS gate is paid
// in parallel (total ~2s) rather than serially. Idempotent across runs.
func seedAccounts(ctx context.Context, cfg Config, rec *metrics.Recorder) {
	const seedConcurrency = 64
	sem := make(chan struct{}, seedConcurrency)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Conns; i++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()
			user := accountName(cfg.UserPrefix, id)
			c, err := proto.Dial(cfg.Addr, 5*time.Second)
			if err != nil {
				rec.Inc("seed_dial_error")
				return
			}
			defer c.Exit()
			if err := c.EnsureAccount(user, cfg.Password, 15*time.Second); err != nil {
				rec.Inc("seed_error")
				return
			}
			rec.Inc("seed_ok")
		}(i)
	}
	wg.Wait()
}

// rampInterval spreads Conns startups across cfg.Ramp.
func rampInterval(cfg Config) time.Duration {
	if cfg.Conns <= 0 || cfg.Ramp <= 0 {
		return 0
	}
	return cfg.Ramp / time.Duration(cfg.Conns)
}

// sleepCtx sleeps for d unless ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
