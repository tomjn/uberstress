package harness

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/tomjn/uberstress/internal/metrics"
	"github.com/tomjn/uberstress/internal/proto"
	"github.com/tomjn/uberstress/internal/workload"
)

// Target identifies one server version to benchmark.
type Target struct {
	Label  string        // human/version label, recorded as the report ref
	SHA    string        // resolved commit SHA (provenance)
	Addr   string        // host:port to drive load against (external mode)
	Server *ServerConfig // non-nil: launch locally; nil: connect to Addr (external)
}

// BenchOptions are the knobs for one benchmark run.
type BenchOptions struct {
	DB           DBConfig
	ResetDB      bool
	Scenario     string
	Load         workload.Config
	Pingers      int
	PingInterval time.Duration
	ResultsDir   string
	ReadyTimeout time.Duration
}

// RunTarget benchmarks a single version end-to-end: reset the DB, launch (or
// connect to) the server, drive load, tear down, and return a tagged report.
func RunTarget(ctx context.Context, target Target, opt BenchOptions) (metrics.Report, error) {
	if opt.ResetDB {
		fmt.Fprintf(os.Stderr, "[%s] resetting database %q\n", target.Label, opt.DB.Name)
		if err := opt.DB.Reset(ctx); err != nil {
			return metrics.Report{}, err
		}
	}

	addr := target.Addr
	if target.Server != nil {
		fmt.Fprintf(os.Stderr, "[%s] launching server from %s on port %d\n", target.Label, target.Server.Dir, target.Server.Port)
		srv, err := LaunchServer(ctx, *target.Server, opt.DB.SQLURL(), opt.ReadyTimeout)
		if err != nil {
			return metrics.Report{}, err
		}
		defer srv.Stop()
		addr = srv.Addr()
	} else {
		fmt.Fprintf(os.Stderr, "[%s] using external server at %s\n", target.Label, addr)
		if err := waitReady(ctx, addr, opt.ReadyTimeout); err != nil {
			return metrics.Report{}, fmt.Errorf("external server not reachable at %s: %w", addr, err)
		}
	}

	sc, ok := workload.Get(opt.Scenario)
	if !ok {
		return metrics.Report{}, fmt.Errorf("unknown scenario %q", opt.Scenario)
	}

	cfg := opt.Load
	cfg.Addr = addr
	rec := metrics.NewRecorder()
	serverVersion := probeServerVersion(addr)

	fmt.Fprintf(os.Stderr, "[%s] running scenario %q (conns=%d)\n", target.Label, opt.Scenario, cfg.Conns)
	start := time.Now()
	if err := workload.Execute(ctx, sc, cfg, opt.Pingers, opt.PingInterval, rec); err != nil {
		fmt.Fprintf(os.Stderr, "[%s] scenario error (reporting partial results): %v\n", target.Label, err)
	}
	elapsed := time.Since(start)

	rep := rec.BuildReport(opt.Scenario, addr, elapsed)
	rep.Ref = target.Label
	rep.CommitSHA = target.SHA
	rep.ServerVersion = serverVersion
	rep.StartedAt = start.UTC().Format(time.RFC3339)
	rep.Params = cfg.Params()

	if opt.ResultsDir != "" {
		if path, err := rep.Save(opt.ResultsDir); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] warning: could not save report: %v\n", target.Label, err)
		} else {
			fmt.Fprintf(os.Stderr, "[%s] saved report to %s\n", target.Label, path)
		}
	}
	return rep, nil
}

// ResolveSHA returns the HEAD commit SHA of a git checkout, or "" if it cannot
// be determined. Runs with the directory as the working dir (no -C).
func ResolveSHA(dir string) string {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// probeServerVersion reads the TASSERVER greeting for report provenance.
func probeServerVersion(addr string) string {
	c, err := proto.Dial(addr, 5*time.Second)
	if err != nil {
		return ""
	}
	defer c.Close()
	return c.ServerInfo
}
