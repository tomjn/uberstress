// Command uberstress is a load generator and A/B harness for uberserver.
//
// Subcommands:
//
//	uberstress load           drive one scenario against an already-running server
//	uberstress bench          reset DB, launch a server version, load it, save a tagged report
//	uberstress list-scenarios list available load scenarios
//	uberstress compare        diff two saved run reports (old vs new)
//
// `bench` collapses the three planes (orchestrate, generate load, server+DB)
// for the local single-host case. For a dedicated environment, run `load`
// inside the environment, orchestrate separately, and `compare` the saved
// reports. Each run is saved as a self-describing JSON report tagged with its
// commit SHA + params, so a version tested earlier can be reused as a baseline.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tomjn/uberstress/internal/harness"
	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
	"github.com/tomjn/uberstress/internal/workload"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "load":
		cmdLoad(os.Args[2:])
	case "bench":
		cmdBench(os.Args[2:])
	case "list-scenarios":
		cmdList()
	case "compare":
		cmdCompare(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `uberstress - load generator and A/B harness for uberserver

usage:
  uberstress load  --scenario <name> --addr host:port [flags]
  uberstress bench --server-dir <uberserver checkout> --ref <label> [flags]
  uberstress list-scenarios
  uberstress compare --old <report.json> --new <report.json>

run "uberstress load -h" or "uberstress bench -h" for flags.
`)
}

func cmdList() {
	fmt.Println("available scenarios:")
	for _, s := range workload.All() {
		fmt.Printf("  %-18s %s\n", s.Name(), s.Describe())
	}
}

// loadFlags holds the scenario/load knobs shared by `load` and `bench`.
type loadFlags struct {
	scenario     *string
	conns        *int
	duration     *time.Duration
	ramp         *time.Duration
	register     *bool
	userPrefix   *string
	password     *string
	channel      *string
	channels     *int
	sayInterval  *time.Duration
	battleHosts  *int
	pingers      *int
	pingInterval *time.Duration
	results      *string
}

func addLoadFlags(fs *flag.FlagSet) *loadFlags {
	return &loadFlags{
		scenario:     fs.String("scenario", "login-storm", "scenario name (see list-scenarios)"),
		conns:        fs.Int("conns", 100, "concurrent connections"),
		duration:     fs.Duration("duration", 30*time.Second, "steady-state hold duration"),
		ramp:         fs.Duration("ramp", 10*time.Second, "spread connection startup over this window"),
		register:     fs.Bool("register", true, "seed (register + confirm) accounts before the timed phase"),
		userPrefix:   fs.String("user-prefix", "uberstress_", "generated account name prefix"),
		password:     fs.String("password", "stresspw", "shared account password"),
		channel:      fs.String("channel", "stress", "base channel name for chat-style scenarios"),
		channels:     fs.Int("channels", 1, "number of distinct channels to spread users across"),
		sayInterval:  fs.Duration("say-interval", time.Second, "per-connection delay between SAY messages"),
		battleHosts:  fs.Int("battle-hosts", 10, "battle scenario: number of connections that host battles over TLS"),
		pingers:      fs.Int("pingers", 2, "dedicated reactor-health PING connections"),
		pingInterval: fs.Duration("ping-interval", 200*time.Millisecond, "interval between health PINGs"),
		results:      fs.String("results", "results", "directory to save the run report"),
	}
}

func (lf *loadFlags) config(addr string) workload.Config {
	return workload.Config{
		Addr:        addr,
		Conns:       *lf.conns,
		Duration:    *lf.duration,
		Ramp:        *lf.ramp,
		Register:    *lf.register,
		UserPrefix:  *lf.userPrefix,
		Password:    *lf.password,
		Channel:     *lf.channel,
		Channels:    *lf.channels,
		SayInterval: *lf.sayInterval,
		BattleHosts: *lf.battleHosts,
	}
}

func cmdLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8200", "lobby server host:port")
	lf := addLoadFlags(fs)
	ref := fs.String("ref", "", "version label for this run (git ref); recorded in the report")
	sha := fs.String("sha", "", "resolved commit SHA; recorded in the report")
	jsonOut := fs.Bool("json", false, "also print the report as JSON to stdout")
	_ = fs.Parse(args)

	sc, ok := workload.Get(*lf.scenario)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q; try: uberstress list-scenarios\n", *lf.scenario)
		os.Exit(2)
	}

	cfg := lf.config(*addr)
	rec := metrics.NewRecorder()
	serverVersion := probeServerVersion(*addr)

	ctx, cancel := signalContext()
	defer cancel()

	start := time.Now()
	fmt.Fprintf(os.Stderr, "running scenario %q against %s (conns=%d ramp=%s duration=%s)\n",
		*lf.scenario, *addr, *lf.conns, *lf.ramp, *lf.duration)
	if err := workload.Execute(ctx, sc, cfg, *lf.pingers, *lf.pingInterval, rec); err != nil {
		fmt.Fprintf(os.Stderr, "scenario error: %v\n", err)
	}
	cancel()
	elapsed := time.Since(start)

	rep := rec.BuildReport(*lf.scenario, *addr, elapsed)
	rep.Ref = *ref
	rep.CommitSHA = *sha
	rep.ServerVersion = serverVersion
	rep.StartedAt = start.UTC().Format(time.RFC3339)
	rep.Params = cfg.Params()

	rep.WriteHuman(os.Stdout)
	if *jsonOut {
		_ = rep.WriteJSON(os.Stdout)
	}
	if path, err := rep.Save(*lf.results); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not save report: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "saved report to %s\n", path)
	}
}

func cmdBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	lf := addLoadFlags(fs)

	// Server acquisition.
	launch := fs.Bool("launch", true, "launch the server locally (false = use --addr external server)")
	serverDir := fs.String("server-dir", "", "uberserver checkout directory (required when --launch)")
	serverPython := fs.String("server-python", "", "python interpreter (default <server-dir>/venv/bin/python3)")
	port := fs.Int("port", 8300, "lobby port for the launched server")
	natport := fs.Int("natport", 8301, "NAT port for the launched server")
	extAddr := fs.String("addr", "127.0.0.1:8300", "external server host:port (when --launch=false)")
	ref := fs.String("ref", "local", "version label recorded as the report ref")
	readyTimeout := fs.Duration("ready-timeout", 60*time.Second, "how long to wait for the server to accept connections")
	compareTo := fs.String("compare-to", "", "path to a prior saved report to diff this run against")

	// Database.
	dbDefault := harness.DefaultDBConfig()
	dbDriver := fs.String("db-driver", dbDefault.Driver, "SQLAlchemy driver (mysql+pymysql or mysql)")
	dbHost := fs.String("db-host", dbDefault.Host, "database host")
	dbPort := fs.Int("db-port", dbDefault.Port, "database port")
	dbUser := fs.String("db-user", dbDefault.User, "database user")
	dbPassword := fs.String("db-password", dbDefault.Password, "database password")
	dbName := fs.String("db-name", dbDefault.Name, "database name (dropped and recreated each run)")
	mysqlBin := fs.String("mysql-bin", dbDefault.MySQLBin, "path to the mysql CLI")
	dbReset := fs.Bool("db-reset", true, "drop and recreate the database before the run")
	_ = fs.Parse(args)

	if _, ok := workload.Get(*lf.scenario); !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q; try: uberstress list-scenarios\n", *lf.scenario)
		os.Exit(2)
	}

	target := harness.Target{Label: *ref}
	if *launch {
		if *serverDir == "" {
			fmt.Fprintln(os.Stderr, "bench --launch requires --server-dir")
			os.Exit(2)
		}
		target.SHA = harness.ResolveSHA(*serverDir)
		target.Server = &harness.ServerConfig{
			Dir:     *serverDir,
			Python:  *serverPython,
			Port:    *port,
			NatPort: *natport,
		}
	} else {
		target.Addr = *extAddr
	}

	opt := harness.BenchOptions{
		DB: harness.DBConfig{
			Driver: *dbDriver, Host: *dbHost, Port: *dbPort,
			User: *dbUser, Password: *dbPassword, Name: *dbName, MySQLBin: *mysqlBin,
		},
		ResetDB:      *dbReset,
		Scenario:     *lf.scenario,
		Load:         lf.config(""),
		Pingers:      *lf.pingers,
		PingInterval: *lf.pingInterval,
		ResultsDir:   *lf.results,
		ReadyTimeout: *readyTimeout,
	}

	ctx, cancel := signalContext()
	defer cancel()

	rep, err := harness.RunTarget(ctx, target, opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench failed: %v\n", err)
		os.Exit(1)
	}
	rep.WriteHuman(os.Stdout)

	if *compareTo != "" {
		oldRep, err := metrics.LoadReport(*compareTo)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not load --compare-to report: %v\n", err)
			return
		}
		fmt.Println()
		metrics.DiffReports(os.Stdout, oldRep, rep)
	}
}

func cmdCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	oldPath := fs.String("old", "", "path to the baseline (old) run report JSON")
	newPath := fs.String("new", "", "path to the new run report JSON")
	_ = fs.Parse(args)
	if *oldPath == "" || *newPath == "" {
		fmt.Fprintln(os.Stderr, "compare requires --old and --new report paths")
		os.Exit(2)
	}
	oldRep, err := metrics.LoadReport(*oldPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading old report: %v\n", err)
		os.Exit(1)
	}
	newRep, err := metrics.LoadReport(*newPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading new report: %v\n", err)
		os.Exit(1)
	}
	metrics.DiffReports(os.Stdout, oldRep, newRep)
}

// signalContext returns a context cancelled on Ctrl-C / SIGTERM.
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Fprintln(os.Stderr, "interrupt: stopping and writing report...")
		cancel()
	}()
	return ctx, cancel
}

// probeServerVersion opens a throwaway connection to read the TASSERVER
// greeting, used as run provenance. Best-effort; returns "" on failure.
func probeServerVersion(addr string) string {
	c, err := proto.Dial(addr, 5*time.Second)
	if err != nil {
		return ""
	}
	defer c.Close()
	return c.ServerInfo
}
