package workload

import (
	"context"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
)

func init() { register(registerStorm{}) }

// registerStorm ramps Conns fresh connections that each perform one REGISTER
// round-trip and then hold open for the rest of the timed phase. It is the
// register-path analogue of login-storm: REGISTER triggers an async account
// INSERT, so under the old synchronous build the burst blocks the reactor,
// which the background pinger detects as elevated PING latency.
//
// Unlike the login-style scenarios it never seeds: the accounts it registers
// must not already exist (a duplicate name returns REGISTRATIONDENIED). A fresh
// database -- as bench provides on every run -- guarantees the Conns names are
// new. cfg.Register is therefore ignored.
type registerStorm struct{}

func (registerStorm) Name() string { return "register-storm" }
func (registerStorm) Describe() string {
	return "ramp N fresh REGISTER round-trips and hold them; targets async register (no seeding)"
}

func (registerStorm) Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error {
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
			runOneRegister(loadCtx, cfg, id, rec)
		}(i)
	}
	wg.Wait()
	return nil
}

func runOneRegister(ctx context.Context, cfg Config, id int, rec *metrics.Recorder) {
	user := accountName(cfg.UserPrefix, id)

	c, err := proto.Dial(cfg.Addr, 5*time.Second)
	if err != nil {
		rec.Inc("dial_error")
		return
	}
	defer c.Exit()

	d, err := c.Register(user, cfg.Password, 15*time.Second)
	if err != nil {
		rec.ObserveError("REGISTER")
		rec.Inc("register_error")
		return
	}
	rec.Observe("REGISTER", d)
	rec.Inc("register_ok")

	// Hold the connection open so the server carries a steady connected-socket
	// count for the rest of the timed phase, matching login-storm's shape.
	<-ctx.Done()
}
