REDIS_URL ?= redis://127.0.0.1:6379

.PHONY: test test-cluster lint fmt vet vulncheck

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
