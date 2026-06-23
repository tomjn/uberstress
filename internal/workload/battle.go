package workload

import (
	"context"
	"sync"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
)

func init() { register(battle{}) }

// battle splits Conns into BattleHosts TLS hosts and the rest as players. Each
// host upgrades to TLS (OPENBATTLE requires it), opens a battle, and holds it
// open for the run. Players then log in, JOINBATTLE one of the open battles, and
// cycle MYBATTLESTATUS / LEAVEBATTLE / re-JOINBATTLE at a paced rate. This drives
// the battle broadcast fan-out (every status change is rebroadcast to the whole
// battle) plus the join/leave membership churn.
//
// Battle ids are server-assigned, so hosts publish the id they get back from
// OPENBATTLE to an in-process channel; the player phase only starts once those
// ids are collected (a player can't join a battle that isn't open yet).
type battle struct{}

func (battle) Name() string { return "battle" }
func (battle) Describe() string {
	return "TLS hosts OPENBATTLE; players JOIN/MYBATTLESTATUS/LEAVE; targets battle broadcast + membership churn"
}

// Battlestatus values toggled by players. They differ in the spectator/player
// mode bit, so each toggle is a real change the server rebroadcasts to the
// battle rather than a no-op.
const (
	statusPlayer = "4194304"
	statusSpec   = "0"
)

func (battle) Run(ctx context.Context, cfg Config, rec *metrics.Recorder) error {
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

	hosts := cfg.BattleHosts
	if hosts < 1 {
		hosts = 1
	}
	if max := cfg.Conns - 1; hosts > max && max >= 1 {
		hosts = max
	}
	players := cfg.Conns - hosts

	var wg sync.WaitGroup

	// Host phase: start the hosts, each of which opens a battle and reports its id
	// (or "" on failure) exactly once before holding the connection open.
	//
	// Host startups are staggered: the server's single reactor thread cannot
	// service overlapping TLS handshakes, and a synchronized burst of STARTTLS
	// resets all but one connection. Real clients never collide like this because
	// they connect at random times; spacing the host upgrades reproduces that.
	// Hosts are setup, not the measured path, so this delay is harmless.
	const hostStagger = 100 * time.Millisecond
	opened := make(chan string, hosts)
	launched := 0
	for i := 0; i < hosts; i++ {
		if loadCtx.Err() != nil {
			break
		}
		if i > 0 {
			sleepCtx(loadCtx, hostStagger)
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runBattleHost(loadCtx, cfg, id, rec, opened)
		}(i)
		launched++
	}
	ids := collectBattleIDs(loadCtx, opened, launched)
	if len(ids) == 0 {
		// No battles opened: players have nothing to join. Let the hosts (if any
		// are holding) run out the clock so OPENBATTLE latency is still recorded.
		wg.Wait()
		return nil
	}

	// Player phase: ramp the remaining connections, each joining a battle.
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
			runBattlePlayer(loadCtx, cfg, id, ids, opInterval, rec)
		}(hosts + j)
	}
	wg.Wait()
	return nil
}

// collectBattleIDs reads exactly want results from opened (one per host) and
// returns the non-empty battle ids. Hosts always report once, so this returns as
// soon as every host has opened-or-failed rather than waiting a fixed delay.
func collectBattleIDs(ctx context.Context, opened <-chan string, want int) []string {
	ids := make([]string, 0, want)
	for i := 0; i < want; i++ {
		select {
		case <-ctx.Done():
			return ids
		case id := <-opened:
			if id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func runBattleHost(ctx context.Context, cfg Config, id int, rec *metrics.Recorder, opened chan<- string) {
	user := accountName(cfg.UserPrefix, id)

	c, err := proto.Dial(cfg.Addr, 5*time.Second)
	if err != nil {
		rec.Inc("host_dial_error")
		opened <- ""
		return
	}
	defer c.Exit()

	if err := c.StartTLS(10 * time.Second); err != nil {
		rec.Inc("tls_error")
		opened <- ""
		return
	}

	if _, ok := loginWithRetry(ctx, c, user, cfg.Password, rec); !ok {
		opened <- ""
		return
	}

	bid, d, err := c.OpenBattle(user, 15*time.Second)
	if err != nil {
		rec.Inc("openbattle_error")
		opened <- ""
		return
	}
	rec.Observe("OPENBATTLE", d)
	rec.Inc("openbattle_ok")
	opened <- bid

	// Hold the battle open for the rest of the run; the battle is destroyed when
	// the host disconnects.
	<-ctx.Done()
}

func runBattlePlayer(ctx context.Context, cfg Config, id int, ids []string, opInterval time.Duration, rec *metrics.Recorder) {
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

	bid := ids[id%len(ids)]
	d, err := c.JoinBattle(bid, 10*time.Second)
	if err != nil {
		rec.Inc("join_error")
		return
	}
	rec.Observe("JOINBATTLE", d)
	rec.Inc("join_ok")

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
				dd       time.Duration
				err      error
			)
			switch seq % 4 {
			case 0:
				key, ctr = "MYBATTLESTATUS", "status"
				dd, err = c.MyBattleStatus(user, statusPlayer, "0", timeout)
			case 1:
				key, ctr = "MYBATTLESTATUS", "status"
				dd, err = c.MyBattleStatus(user, statusSpec, "0", timeout)
			case 2:
				key, ctr = "LEAVEBATTLE", "leave"
				dd, err = c.LeaveBattle(bid, user, timeout)
			default:
				key, ctr = "JOINBATTLE", "join"
				dd, err = c.JoinBattle(bid, timeout)
			}
			if err != nil {
				rec.Inc(ctr + "_error")
				return
			}
			rec.Observe(key, dd)
			rec.Inc(ctr + "_ok")
			seq++
		}
	}
}
