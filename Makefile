REDIS_URL ?= redis://127.0.0.1:6379

.PHONY: test test-cluster lint fmt vet vulncheck bench perfgate

test: ## Run the full suite against a local Redis (race detector on)
	REDIS_URL=$(REDIS_URL) go test -race -shuffle=on -count=1 -timeout 30m ./...

test-cluster: ## Also run the Redis Cluster tests (nodes on 7000-7002)
	REDIS_URL=$(REDIS_URL) REDIS_CLUSTER_NODES=redis://127.0.0.1:7000,redis://127.0.0.1:7001,redis://127.0.0.1:7002 \
		go test -race -count=1 -timeout 30m ./...

lint:
	golangci-lint run ./...

fmt:
	golangci-lint fmt ./...

vet:
	go vet ./...

vulncheck:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

BENCH_OUT ?= bench.out

bench: ## Run the benchmarks (use a memory-capped Redis, see benchmark_test.go)
	REDIS_URL=$(REDIS_URL) go test -run '^$$' -bench . -benchtime 3000x -benchmem -count 3 . | tee $(BENCH_OUT)

perfgate: bench ## Check the allocation budgets the CI enforces (add BASE=old.out to compare throughput)
	go run ./.github/perfgate -head $(BENCH_OUT) $(if $(BASE),-base $(BASE)) -budget .github/perfgate/budget.json
