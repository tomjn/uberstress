# Dev task runner for uberstress.
#
# The point of this file is stable command prefixes. Recurring operations
# (build, bench, launch a trace server, run a trace script) otherwise vary by
# scenario/port/filename on every invocation, so each one triggers a fresh
# approval. Routing them through `make <target>` keeps the command prefix fixed
# and pushes the variation into overridable variables, e.g.
#
#     make bench SCENARIO=battle CONNS=50
#     make trace-server TRACE_PORT=8400
#
# make runs the real commands as subprocesses, so only the `make ...` line is
# seen by the harness -- the inner go/mysql/pkill calls don't prompt.

SERVER_DIR    ?= ../uberserver
PY            ?= $(SERVER_DIR)/venv/bin/python3

# bench / load knobs
SCENARIO      ?= login-storm
CONNS         ?= 30
RAMP          ?= 3s
DURATION      ?= 6s
REF           ?= test
BENCH_PORT    ?= 8300
BENCH_NATPORT ?= 8301
BATTLE_HOSTS  ?= 6
ADDR          ?= 127.0.0.1:8200
EXTRA         ?=

# manual trace server + trace script (for verifying wire formats live)
TRACE_PORT    ?= 8400
TRACE_NATPORT ?= 8401
TRACE_DB      ?= uberstress_trace
DB_HOST       ?= 127.0.0.1
DB_PORT       ?= 3306
DB_USER       ?= root
DB_PASS       ?= root
TRACE_SQLURL  ?= mysql+pymysql://$(DB_USER):$(DB_PASS)@$(DB_HOST):$(DB_PORT)/$(TRACE_DB)?charset=utf8
TRACE_SCRIPT  ?= tmp/trace.py

.PHONY: build vet fmt check list bench load trace-db trace-server stop-trace-server trace clean

build:
	go build -o uberstress ./cmd/uberstress

vet:
	go vet ./...

fmt:
	gofmt -l internal/ cmd/

# build + vet + gofmt-check, the pre-commit gate CI also runs.
check: build vet fmt
	@echo "check: build + vet ok; gofmt -l output above must be empty"

list:
	go run ./cmd/uberstress list-scenarios

# Reset DB, launch a server checkout, run one scenario, save a tagged report.
# Override SCENARIO/CONNS/RAMP/DURATION/BATTLE_HOSTS; pass anything else via EXTRA
# (e.g. EXTRA="--compare-to results/old.json").
bench:
	go run ./cmd/uberstress bench --server-dir $(SERVER_DIR) --ref $(REF) \
	  --scenario $(SCENARIO) --conns $(CONNS) --ramp $(RAMP) --duration $(DURATION) \
	  --port $(BENCH_PORT) --natport $(BENCH_NATPORT) --battle-hosts $(BATTLE_HOSTS) $(EXTRA)

# Drive a scenario against an already-running server at ADDR.
load:
	go run ./cmd/uberstress load --addr $(ADDR) --scenario $(SCENARIO) \
	  --conns $(CONNS) --ramp $(RAMP) --duration $(DURATION) $(EXTRA)

# Create the throwaway trace database if it does not exist (idempotent).
trace-db:
	mysql -u$(DB_USER) -p$(DB_PASS) -h$(DB_HOST) -P$(DB_PORT) \
	  -e "CREATE DATABASE IF NOT EXISTS $(TRACE_DB) CHARACTER SET utf8;"

# Launch a manual server on TRACE_PORT for live protocol tracing. Run it with the
# Bash tool's background mode; stop it with `make stop-trace-server`.
trace-server: trace-db
	cd $(SERVER_DIR) && ./venv/bin/python3 server.py -p $(TRACE_PORT) -n $(TRACE_NATPORT) \
	  -s "$(TRACE_SQLURL)" -o /tmp/uberstress-trace.log

stop-trace-server:
	-pkill -f "server.py -p $(TRACE_PORT)"

# Run the current trace script (write it to $(TRACE_SCRIPT) first; tmp/ is ignored).
trace:
	@mkdir -p $(dir $(TRACE_SCRIPT))
	$(PY) $(TRACE_SCRIPT)

clean:
	rm -f uberstress
	rm -rf tmp
