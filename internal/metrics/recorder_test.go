package metrics

import (
	"testing"
	"time"
)

func TestBuildReportErrorCount(t *testing.T) {
	r := NewRecorder()
	r.Observe("LOGIN", 5*time.Millisecond)
	r.Observe("LOGIN", 7*time.Millisecond)
	r.ObserveError("LOGIN")
	// A command that only ever failed must still appear, with Count 0.
	r.ObserveError("PING")
	r.ObserveError("PING")

	rep := r.BuildReport("login-storm", "127.0.0.1:8200", 10*time.Second)

	byCmd := map[string]CmdStat{}
	for _, c := range rep.Commands {
		byCmd[c.Command] = c
	}

	login, ok := byCmd["LOGIN"]
	if !ok {
		t.Fatal("LOGIN missing from report")
	}
	if login.Count != 2 || login.ErrorCount != 1 {
		t.Errorf("LOGIN: got count=%d error_count=%d, want 2/1", login.Count, login.ErrorCount)
	}

	ping, ok := byCmd["PING"]
	if !ok {
		t.Fatal("PING (errors-only) missing from report")
	}
	if ping.Count != 0 || ping.ErrorCount != 2 {
		t.Errorf("PING: got count=%d error_count=%d, want 0/2", ping.Count, ping.ErrorCount)
	}
}

func TestSnapshot(t *testing.T) {
	r := NewRecorder()
	r.Observe("LOGIN", time.Millisecond)
	r.Observe("PING", time.Millisecond)
	r.Observe("PING", time.Millisecond)
	r.Inc("dial_error")
	r.Inc("login_error")
	r.Inc("login_retry") // retries are not errors
	r.Inc("login_ok")    // successes are not errors

	commands, errors := r.Snapshot()
	if commands != 3 {
		t.Errorf("commands: got %d, want 3", commands)
	}
	if errors != 2 {
		t.Errorf("errors: got %d, want 2 (dial_error + login_error)", errors)
	}
}
