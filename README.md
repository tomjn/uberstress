# uberstress

A standalone load generator and A/B harness for [uberserver](https://github.com/spring/uberserver).
It measures the difference between two versions of the server — for example
before and after a set of changes — by generating concurrent load over the
lobby wire protocol and comparing latency under that load.

It speaks the protocol directly, so one binary drives both versions unchanged.

## Why this exists

uberserver runs a single-threaded event loop (the Twisted reactor). If a server
version performs its database work synchronously on that loop, one slow DB call
blocks every connected client until it returns — head-of-line blocking. Other
versions move that work to a worker thread pool so the loop stays responsive.

`uberstress` generates enough concurrent DB-bound load to expose that
difference and reports the delta between two versions.

### Measure against MariaDB, not SQLite

The comparison is **only meaningful against MariaDB**. With SQLite the server
forces its DB worker pool to a single thread, which masks any difference between
versions. Run both server versions against MariaDB. Because MariaDB typically
shares the machine with the server (and possibly this load generator), treat
results as **relative deltas between versions**, not absolute throughput, and
pin the generator's CPU (`GOMAXPROCS`, `taskset`) so it doesn't distort the
measurement.

## Build

```sh
go build -o uberstress ./cmd/uberstress
```

Requires Go 1.26+. No external dependencies.

## Usage

```sh
# List load scenarios
uberstress list-scenarios

# Run one scenario against an already-running server, saving a report
uberstress load --scenario login-storm --addr 127.0.0.1:8200 \
    --conns 500 --ramp 10s --duration 30s --ref new --sha "$(git rev-parse HEAD)"

# Local all-in-one: reset the DB, launch a server checkout, load it, save a
# tagged report -- and diff it against a previously-saved baseline.
uberstress bench --server-dir ../uberserver --ref my-change \
    --scenario login-storm --conns 500 --ramp 10s --duration 30s \
    --compare-to results/login-storm__<baseline-sha>.json

# Diff two saved reports (old vs new)
uberstress compare --old results/login-storm__<old>.json \
                   --new results/login-storm__<new>.json
```

### Database and server configuration

`bench` defaults to a local Homebrew MariaDB (`root`/`root` over TCP) and the
`mysql+pymysql` SQLAlchemy driver (uberserver's dev venv ships PyMySQL, not
`mysqlclient`). Everything is a flag — `--db-host`, `--db-port`, `--db-user`,
`--db-password`, `--db-name`, `--db-driver`, `--mysql-bin` — and `--launch=false
--addr host:port` points at an already-running (e.g. remote) server instead of
spawning one. The database is dropped and recreated before each run via the
`mysql` CLI so every version starts from an identical clean schema.

### Reusing a saved run

Every `load` run is saved as a self-describing JSON report tagged with its
`ref`, resolved `commit_sha`, server version, and scenario params. When you make
another round of changes you do **not** re-run the versions already tested
earlier — pass the previously-saved report to `compare --old` and only re-run
the version that changed.

## How it works (design notes)

- **Request/response serialization, not `#id` correlation.** The protocol's
  `#id` echo is only attached when the reply is produced on the handler thread
  (`Protocol.py:382-385`), which is not the case for asynchronous/deferred
  responses. Instead each connection sends one command and times the wait for
  its expected response token. Load scales via many connections — which is what
  stresses the single reactor anyway.

- **The PING prober is the headline metric.** A few dedicated connections issue
  `PING` at a fixed interval throughout a run. `PING` is allowed pre-login and
  touches no database, so its latency is a near-direct readout of reactor
  head-of-line blocking: flat when DB work is off the loop, spiking when a DB
  call on the loop holds it up.

- **Two-phase accounts.** A seed phase registers accounts and confirms the
  agreement (absorbing the server's mandatory ~2s "read the terms" gate, paid in
  parallel). The timed phase then logs into already-confirmed accounts, so the
  gate never pollutes the measured login latency.

- **Counters are first-class.** Errors, timeouts, login retries and flood-kicks
  are reported alongside latency — a "fast" run that silently dropped
  connections is not a win.

## Status

Implemented and verified end-to-end against MariaDB: protocol client, metrics,
reactor pinger, report save/load, `compare`, the `bench` local A/B harness
(reset DB, launch a server checkout, load, teardown, tagged report,
`--compare-to`), and the `login-storm` and `chat` scenarios.

Planned:

- Scenarios: `register-storm`, `social`, `battle` (needs STARTTLS, as
  `OPENBATTLE` requires TLS), `battle-list + login-storm` (combined), `mixed`.
- A two-target mode that benchmarks old and new in one invocation (currently:
  run `bench` per version, or reuse a saved report via `--compare-to`).
- The old version's git ref must boot under the same Python/deps as the new one;
  if it doesn't, build a branch that combines the old code with only the
  dependency-compatibility commits to compare against.
