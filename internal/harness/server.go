package harness

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// ServerConfig describes how to launch a local uberserver subprocess. For the
// remote case this is left nil and the harness connects to an already-running
// server instead (external mode).
type ServerConfig struct {
	Dir     string   // uberserver checkout directory (cmd working dir)
	Python  string   // python interpreter; defaults to <Dir>/venv/bin/python3
	Port    int      // lobby TCP port
	NatPort int      // NAT UDP port
	Extra   []string // any additional server.py args
}

// Server is a running uberserver subprocess.
type Server struct {
	cmd  *exec.Cmd
	addr string
}

// LaunchServer starts uberserver pointed at sqlurl and waits until its lobby
// port accepts connections. The caller must Stop it.
func LaunchServer(ctx context.Context, sc ServerConfig, sqlurl string, ready time.Duration) (*Server, error) {
	python := sc.Python
	if python == "" {
		python = filepath.Join(sc.Dir, "venv", "bin", "python3")
	}
	args := []string{
		"server.py",
		"-p", strconv.Itoa(sc.Port),
		"-n", strconv.Itoa(sc.NatPort),
		"-s", sqlurl,
	}
	args = append(args, sc.Extra...)

	cmd := exec.Command(python, args...)
	cmd.Dir = sc.Dir
	cmd.Stdout = os.Stderr // server's own logging goes to its server.log; surface stray stdout/err
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting server: %w", err)
	}

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(sc.Port))
	srv := &Server{cmd: cmd, addr: addr}
	if err := waitReady(ctx, addr, ready); err != nil {
		srv.Stop()
		return nil, fmt.Errorf("server did not become ready on %s: %w", addr, err)
	}
	return srv, nil
}

// Addr is the host:port the server listens on.
func (s *Server) Addr() string { return s.addr }

// Stop sends SIGTERM and waits briefly, then kills if it has not exited.
func (s *Server) Stop() {
	if s.cmd == nil || s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = s.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	}
}

// waitReady polls addr until a TCP connection succeeds or timeout elapses.
func waitReady(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s", timeout)
}
