package metrics

import (
	"fmt"
	"io"
	"sort"
)

// DiffReports prints an old-vs-new comparison table. The headline rows are the
// reactor-health proxy (PING) and time-to-login (LOGIN); a regression on the
// old build should show markedly higher p99 there under the same load.
func DiffReports(w io.Writer, old, new Report) {
	fmt.Fprintf(w, "A/B comparison: scenario=%s\n", new.Scenario)
	fmt.Fprintf(w, "  old: ref=%s sha=%s ver=%s\n", dash(old.Ref), shortSHA(old.CommitSHA), dash(old.ServerVersion))
	fmt.Fprintf(w, "  new: ref=%s sha=%s ver=%s\n", dash(new.Ref), shortSHA(new.CommitSHA), dash(new.ServerVersion))
	fmt.Fprintf(w, "%-16s %12s %12s %12s %10s\n", "command/metric", "old_p99ms", "new_p99ms", "delta_ms", "change")

	oldByCmd := index(old.Commands)
	newByCmd := index(new.Commands)

	cmds := union(oldByCmd, newByCmd)
	for _, name := range cmds {
		o, oOK := oldByCmd[name]
		n, nOK := newByCmd[name]
		var oP99, nP99 float64
		if oOK {
			oP99 = o.P99ms
		}
		if nOK {
			nP99 = n.P99ms
		}
		fmt.Fprintf(w, "%-16s %12.2f %12.2f %12.2f %10s\n",
			name, oP99, nP99, nP99-oP99, pct(oP99, nP99))
	}

	// Counter deltas (errors, timeouts, flood-kicks) matter: a "fast" run that
	// silently dropped connections is not a win.
	keys := unionCounters(old.Counters, new.Counters)
	if len(keys) > 0 {
		fmt.Fprintf(w, "counters (old -> new):\n")
		for _, k := range keys {
			fmt.Fprintf(w, "  %-24s %d -> %d\n", k, old.Counters[k], new.Counters[k])
		}
	}
}

func index(cs []CmdStat) map[string]CmdStat {
	m := make(map[string]CmdStat, len(cs))
	for _, c := range cs {
		m[c.Command] = c
	}
	return m
}

func union(a, b map[string]CmdStat) []string {
	set := make(map[string]struct{})
	for k := range a {
		set[k] = struct{}{}
	}
	for k := range b {
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unionCounters(a, b map[string]int64) []string {
	set := make(map[string]struct{})
	for k := range a {
		set[k] = struct{}{}
	}
	for k := range b {
		set[k] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func pct(old, new float64) string {
	if old == 0 {
		if new == 0 {
			return "0%"
		}
		return "new"
	}
	return fmt.Sprintf("%+.0f%%", (new-old)/old*100)
}
