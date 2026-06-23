# uberstress

A standalone load generator and A/B harness for [uberserver](https://github.com/spring/uberserver),
built to quantify the difference its performance roadmap made — specifically the
Phase 3 work that moved synchronous database calls off the single Twisted
reactor thread onto a worker pool.

It speaks the lobby wire protocol directly, so one binary drives both the old
(pre-roadmap) and new server unchanged.

## Why this exists

Under the **old** server, one slow DB call (login, register, say-history,
ignore, friend, mystatus) blocks the entire reactor: every connected client
stalls until it returns. The roadmap's claim is that the **new** server keeps
the reactor responsive under DB load. `uberstress` generates enough concurrent
DB-bound load to expose that head-of-line blocking and reports the delta.

### Measure against MariaDB, not SQLite

The comparison is **only meaningful against MariaDB**. With SQLite the server
forces its DB worker pool to a single thread (`DataHandler.py`), which
neutralizes the async work — old and new measure nearly identically. Run both
server versions against MariaDB. Because MariaDB typically shares the machine
with the server (and possibly this load generator), treat results as **relative
old-vs-new deltas**, not absolute throughput, and pin the generator's CPU
(`GOMAXPROCS`, `taskset`) so it doesn't distort the measurement.

## Build

```sh
go build -o uberstress ./cmd/uberstress
```

Requires Go 1.26+. No external dependencies.

## Usage

```sh
# List load scenarios
uberstress list-scenarios

# Run one scenario against a running server, saving a report
uberstress load --scenario login-storm --addr 127.0.0.1:8200 \
    --conns 500 --ramp 10s --duration 30s --ref new --sha "$(git rev-parse HEAD)"

# Diff two saved reports (old vs new)
uberstress compare --old results/login-storm__<old>.json \
                   --new results/login-storm__<new>.json
```

### Reusing a baseline run

Every `load` run is saved as a self-describing JSON report tagged with its
`ref`, resolved `commit_sha`, server version, and scenario params. When you make
a third round of changes you do **not** re-run the versions already tested
earlier — pass the previously-saved report to `compare --old` and only re-run
the ref that changed.

## How it works (design notes)

- **Request/response serialization, not `#id` correlation.** The protocol's
  `#id` echo is only attached when the reply is produced on the handler thread
  (`Protocol.py:382-385`), which is *not* the case for the async/deferred
  responses we most want to measure. Instead each connection sends one command
  and times the wait for its expected response token. Load scales via many
  connections — which is what stresses the single reactor anyway.

- **The PING prober is the headline metric.** A few dedicated connections issue
  `PING` at a fixed interval throughout a run. `PING` is allowed pre-login and
  touches no database, so its latency is a near-direct readout of reactor
  head-of-line blocking: flat on the async build, spiking on the old build
  whenever a DB call is in flight.

- **Two-phase accounts.** A seed phase registers accounts and confirms the
  agreement (absorbing the server's mandatory ~2s "read the terms" gate, paid in
  parallel). The timed phase then logs into already-confirmed accounts, so the
  2s gate never pollutes the measured login latency.

- **Counters are first-class.** Errors, timeouts, login retries and flood-kicks
  are reported alongside latency — a "fast" run that silently dropped
  connections is not a win.

## Status

Implemented and verified end-to-end (protocol client, metrics, reactor pinger,
`login-storm`, report save/load, `compare`).

Planned (see the project plan):

- Scenarios: `register-storm`, `chat`, `social`, `battle` (needs STARTTLS, as
  `OPENBATTLE` requires TLS), `battle-list + login-storm` (combined), `mixed`.
- `compare` orchestration that launches both server versions against MariaDB,
  resets the DB between runs, and skips any commit SHA already tested.
- Baseline ref must boot under the same Python/deps as HEAD; if not, build a
  `baseline-perf` branch (baseline + compat-only commits) to compare against.
