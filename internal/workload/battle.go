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

	opInterval := cfg.SayInterval
	if opInterval <= 0 {
		opInterval = time.Second
	}

	hosts := battleHostCount(cfg)
	players := cfg.Conns - hosts

	var wg sync.WaitGroup

	// Setup phase (not measured): open battles and hold them. They hold on hostCtx
	// so they can be released once the measured player phase ends, independent of
	// how long sequential host setup took.
	hostCtx, releaseHosts := context.WithCancel(ctx)
	defer releaseHosts()
	ids := startBattleHosts(hostCtx, cfg, hosts, rec, &wg)
	if len(ids) == 0 {
		// No battles opened: players have nothing to join.
		releaseHosts()
		wg.Wait()
		return nil
	}

	// Measured phase: ramp the remaining connections, each joining a battle. Its
	// window starts now, after setup, so host setup never eats the load budget.
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
			runBattlePlayer(loadCtx, cfg, id, ids, opInterval, rec)
		}(hosts + j)
	}

	<-loadCtx.Done() // let the measured window elapse
	releaseHosts()   // then release the held battle hosts
	wg.Wait()
	return nil
}

// battleHostCount clamps BattleHosts to [1, Conns-1] so there is at least one
// host and (when possible) at least one non-host connection.
func battleHostCount(cfg Config) int {
	hosts := cfg.BattleHosts
	if hosts < 1 {
		hosts = 1
	}
	if max := cfg.Conns - 1; hosts > max && max >= 1 {
		hosts = max
	}
	return hosts
}

// startBattleHosts brings up hosts TLS battle hosts and returns the opened
// battle ids. Each host opens a battle and then holds it open for the run; the
// holding goroutines are tracked in wg.
//
// Setup is sequential: host i+1 is not started until host i reports (via the
// opened channel) that it has opened-or-failed. The server's single reactor
// thread cannot service overlapping TLS handshakes -- a concurrent burst of
// STARTTLS resets all but one connection, and even a fixed stagger is not enough
// once battle-open broadcasts start competing for the reactor. Serializing the
// upgrades sidesteps it entirely. Real clients never collide like this (they
// connect at random times), and hosts are setup rather than the measured path,
// so the cost is harmless.
func startBattleHosts(ctx context.Context, cfg Config, hosts int, rec *metrics.Recorder, wg *sync.WaitGroup) []string {
	opened := make(chan string, 1)
	ids := make([]string, 0, hosts)
	for i := 0; i < hosts; i++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runBattleHost(ctx, cfg, id, rec, opened)
		}(i)
		// Wait for this host to open (or fail) before starting the next, so no two
		// TLS handshakes overlap on the reactor.
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

// dialTLSHost dials and upgrades to TLS, retrying on the transient handshake
// resets the single reactor produces under load (no server-side error is logged;
// the connection is simply reset). Returns nil once attempts are exhausted or
// the context is cancelled.
func dialTLSHost(ctx context.Context, cfg Config, rec *metrics.Recorder) *proto.Client {
	for attempt := 0; attempt < 6; attempt++ {
		if ctx.Err() != nil {
			return nil
		}
		c, err := proto.Dial(cfg.Addr, 5*time.Second)
		if err != nil {
			rec.Inc("host_dial_error")
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}
		if err := c.StartTLS(10 * time.Second); err != nil {
			rec.Inc("tls_retry")
			c.Close()
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}
		return c
	}
	return nil
}

func runBattleHost(ctx context.Context, cfg Config, id int, rec *metrics.Recorder, opened chan<- string) {
	user := accountName(cfg.UserPrefix, id)

	c := dialTLSHost(ctx, cfg, rec)
	if c == nil {
		rec.Inc("tls_error")
		opened <- ""
		return
	}
	defer c.Exit()

	if _, ok := loginWithRetry(ctx, c, user, cfg.Password, rec); !ok {
		opened <- ""
		return
	}

	bid, d, err := c.OpenBattle(user, 15*time.Second)
	if err != nil {
		rec.ObserveError("OPENBATTLE")
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
		rec.ObserveError("JOINBATTLE")
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
				rec.ObserveError(key)
				rec.Inc(ctr + "_error")
				return
			}
			rec.Observe(key, dd)
			rec.Inc(ctr + "_ok")
			seq++
		}
	}
}
