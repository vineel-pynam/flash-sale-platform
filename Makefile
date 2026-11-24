# Run from the repository root. Requires Docker, Go 1.26+ and (for load tests) k6.
# Integration tests read REDIS_ADDR and DATABASE_URL from .env (copy .env.example first).

# pipefail: a failing test fails the target even when its output is piped through grep.
SHELL       := /bin/bash
.SHELLFLAGS := -eo pipefail -c
SVC         := reservation-service
ENV         := set -a && source .env && set +a

.PHONY: up down test test-unit test-race bench run token verify loadtest-spike

up:            ## start Redis and Postgres
	docker compose up -d --wait

down:          ## stop and remove containers
	docker compose down -v

test-unit:     ## unit tests only, no Docker needed
	cd $(SVC) && go test -short ./...

test: up       ## full suite incl. 200K-attempt concurrency test and e2e money-path test
	$(ENV) && cd $(SVC) && go test -p 1 -count=1 -v ./... | grep -E '^(=== RUN|--- |ok|FAIL|PASS)|attempts in|invariants|paid='

test-race: up
	$(ENV) && cd $(SVC) && go test -race -p 1 -count=1 ./...

bench: up
	$(ENV) && cd $(SVC) && go test -run '^$$' -bench BenchmarkReserve -benchtime 50000x -cpu 1,8,32 ./internal/inventory/

# Sales are kept in Postgres, so a sold-out drop stays sold out. Use a new DROP for a fresh run.
DROP_ENV    = $(if $(DROP),DROP_ID=$(DROP)) $(if $(UNITS),DROP_UNITS=$(UNITS))

run: up        ## run the API on :8080; saves the drop in Postgres and loads Redis from it (make run DROP=spike-1 UNITS=5000)
	$(ENV) && cd $(SVC) && $(DROP_ENV) go run ./cmd/server -seed

token:         ## print an admission token: make token USER=alice
	$(ENV) && cd $(SVC) && go run ./cmd/token -user $(USER)

verify:        ## correctness check after a load test (use the same DROP as make run)
	$(ENV) && cd $(SVC) && $(DROP_ENV) go run ./cmd/verify

loadtest-spike: ## k6 spike profile against a running server (make run in another terminal; same DROP)
	$(ENV) && $(DROP_ENV) k6 run loadtest/spike.js
