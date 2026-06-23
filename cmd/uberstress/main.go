// Command uberstress is a load generator and A/B harness for uberserver.
//
// Subcommands:
//
//	uberstress load           run one scenario against a running server
//	uberstress list-scenarios list available load scenarios
//	uberstress compare        diff two saved run reports (old vs new)
//
// The compare/harness orchestration (spawning server versions against MariaDB)
// is layered on top of `load`: each run is saved as a self-describing JSON
// report tagged with its commit SHA + params, so a version tested earlier can
// be reused as a baseline instead of being re-run.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
  uberstress load --scenario <name> --addr host:port [flags]
  uberstress list-scenarios
  uberstress compare --old <report.json> --new <report.json>

run "uberstress load -h" for load flags.
`)
}

func cmdList() {
	fmt.Println("available scenarios:")
	for _, s := range workload.All() {
		fmt.Printf("  %-18s %s\n", s.Name(), s.Describe())
	}
}

func cmdLoad(args []string) {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8200", "lobby server host:port")
	scenario := fs.String("scenario", "login-storm", "scenario name (see list-scenarios)")
	conns := fs.Int("conns", 100, "concurrent connections")
	duration := fs.Duration("duration", 30*time.Second, "steady-state hold duration")
	ramp := fs.Duration("ramp", 10*time.Second, "spread connection startup over this window")
	register := fs.Bool("register", true, "register accounts before login")
	userPrefix := fs.String("user-prefix", "uberstress_", "generated account name prefix")
	password := fs.String("password", "stresspw", "shared account password")
	channel := fs.String("channel", "stress", "channel for chat-style scenarios")
	pingers := fs.Int("pingers", 2, "dedicated reactor-health PING connections")
	pingInterval := fs.Duration("ping-interval", 200*time.Millisecond, "interval between health PINGs")
	resultsDir := fs.String("results", "results", "directory to save the run report")
	ref := fs.String("ref", "", "version label for this run (git ref); recorded in the report")
	sha := fs.String("sha", "", "resolved commit SHA; recorded in the report")
	jsonOut := fs.Bool("json", false, "also print the report as JSON to stdout")
	_ = fs.Parse(args)

	sc, ok := workload.Get(*scenario)
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario %q; try: uberstress list-scenarios\n", *scenario)
		os.Exit(2)
	}

	cfg := workload.Config{
		Addr:       *addr,
		Conns:      *conns,
		Duration:   *duration,
		Ramp:       *ramp,
		Register:   *register,
		UserPrefix: *userPrefix,
		Password:   *password,
		Channel:    *channel,
	}

	rec := metrics.NewRecorder()
	serverVersion := probeServerVersion(*addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel on Ctrl-C for a clean shutdown + report.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Fprintln(os.Stderr, "interrupt: stopping load and writing report...")
		cancel()
	}()

	// The scenario owns its own timing (seed phase + ramp + duration); main
	// only cancels on Ctrl-C. Pingers run for the whole scenario.

	// Background reactor-health pingers.
	var pingWG sync.WaitGroup
	for i := 0; i < *pingers; i++ {
		pingWG.Add(1)
		go func() {
			defer pingWG.Done()
			workload.RunPinger(ctx, *addr, *pingInterval, rec)
		}()
	}

	start := time.Now()
	fmt.Fprintf(os.Stderr, "running scenario %q against %s (conns=%d ramp=%s duration=%s)\n",
		*scenario, *addr, *conns, *ramp, *duration)
	if err := sc.Run(ctx, cfg, rec); err != nil {
		fmt.Fprintf(os.Stderr, "scenario error: %v\n", err)
	}
	cancel()
	pingWG.Wait()
	elapsed := time.Since(start)

	rep := rec.BuildReport(*scenario, *addr, elapsed)
	rep.Ref = *ref
	rep.CommitSHA = *sha
	rep.ServerVersion = serverVersion
	rep.StartedAt = start.UTC().Format(time.RFC3339)
	rep.Params = cfg.Params()

	rep.WriteHuman(os.Stdout)
	if *jsonOut {
		_ = rep.WriteJSON(os.Stdout)
	}
	if path, err := rep.Save(*resultsDir); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not save report: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "saved report to %s\n", path)
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
