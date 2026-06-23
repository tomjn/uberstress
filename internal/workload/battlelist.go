package workload

import (
	"context"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
)

func init() { register(battleList{}) }

// battleList opens many battles up front (BattleHosts TLS hosts, each holding a
// battle) and then runs a login storm over the remaining accounts. The point is
// the login state dump: the server sends a BATTLEOPENED (plus UPDATEBATTLEINFO)
// line for every open battle to each logging-in client, so a large battle list
// inflates every login's dump. On the old synchronous build that work happens on
// the reactor, so this is the sharpest old-vs-new discriminator -- LOGIN latency
// and the background PING both feel the long dump under contention; on the async
// build they stay flat.
//
// Crank --battle-hosts to lengthen the battle list (and thus the dump). The
// remaining Conns-BattleHosts connections are the login storm.
type battleList struct{}

func (battleList) Name() string { return "battle-list" }
func (battleList) Describe() string {
	return "open many battles, then login-storm so each login's state dump carries the full battle list"
}

func (battleList) Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error {
	if cfg.Register {
		seedAccounts(ctx, cfg, rec)
		sleepCtx(ctx, 500*time.Millisecond)
	}

	hosts := battleHostCount(cfg)
	players := cfg.Conns - hosts

	var wg sync.WaitGroup

	// Setup phase (not measured): open the battles and hold them so every login
	// dump carries the full list. Held on hostCtx, released once the storm ends.
	// Sequential host setup can be slow for a long battle list, so keeping it out
	// of the measured window matters here especially.
	hostCtx, releaseHosts := context.WithCancel(ctx)
	defer releaseHosts()
	ids := startBattleHosts(hostCtx, cfg, hosts, rec, &wg)
	rec.Add("battles_open", int64(len(ids)))

	// Measured phase: login storm over the remaining accounts. Each fresh login
	// receives the BATTLEOPENED list built above. Its window starts now.
	loadCtx, cancel := context.WithTimeout(ctx, cfg.Ramp+cfg.Duration)
	defer cancel()

	interval := time.Duration(0)
	if players > 0 && cfg.Ramp > 0 {
		interval = cfg.Ramp / time.Duration(players)
	}
	for j := 0; j < players; j++ {
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
		}(hosts + j)
	}

	<-loadCtx.Done() // let the measured login storm elapse
	releaseHosts()   // then release the held battle hosts
	wg.Wait()
	return nil
}
