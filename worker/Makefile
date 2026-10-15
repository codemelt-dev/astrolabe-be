GO      ?= go
VERSION ?= $(shell git -C .. describe --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
BIN     := bin

.PHONY: build build-linux test test-live vet fmt check clean alerts

# Native build (the development Mac).
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/worker .

# Production build for the Galexie host (installed as worker-<version>).
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/worker-linux-amd64 .

# DB tests use TEST_DATABASE_URL and skip when it is unset. The lake's
# latency assertions (long-poll wake-up within 5 ms) run on their own first,
# so the CPU-heavy pipeline tests (ingest, kill -9, reindex) cannot starve
# them.
test:
	$(GO) test -race ./lake/...
	$(GO) test -race $$($(GO) list ./... | grep -v '/lake')

# Opt-in network tests against the AWS public lake.
#   M2: decode and validate the newest 2,000 ledgers (~400 MB cached in
#   LAKE_CACHE, deleted afterwards), feed them through dev-feed into a lake at
#   accelerated cadence, and run `worker dev-feed` at ledger cadence on the
#   live tip.
#   M3 (needs TEST_DATABASE_URL): replay 2,000 recent ledgers into a scratch
#   database (verified in chunks of 500, ~1 GB at a time), rerun, reindex 500;
#   then dev-feed at the live tip with ingest following, latency measured.
LAKE_CACHE ?= /tmp/worker-lake-cache
test-live:
	WORKER_TEST_AWS_LEDGERS=2000 WORKER_TEST_LAKE_CACHE=$(LAKE_CACHE) $(GO) test -count=1 -timeout 30m -v -run TestAWSLakeRecentLedgers ./sep54
	WORKER_TEST_LAKE_CACHE=$(LAKE_CACHE) $(GO) test -count=1 -timeout 30m -v -run TestFeedCachedAWSLedgers ./devfeed
	WORKER_TEST_LIVE_AWS=1 $(GO) test -count=1 -timeout 10m -v -run TestCLIDevFeedLiveAWS .
	rm -rf $(LAKE_CACHE)
	WORKER_TEST_AWS_REPLAY=2000 $(GO) test -count=1 -timeout 60m -v -run TestLiveReplayAWS ./ingest
	WORKER_TEST_LIVE_AWS=1 $(GO) test -count=1 -timeout 20m -v -run TestLiveDevFeedLatency ./ingest

vet:
	$(GO) vet ./...
	GOOS=linux GOARCH=amd64 $(GO) vet ./...

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt vet test build-linux

clean:
	rm -rf $(BIN)

# Alert rules (../galexie/deploy/alerts.yml) and their simulations; needs promtool
# (part of the Prometheus release). Not part of `check`.
alerts:
	promtool check rules ../galexie/deploy/alerts.yml
	cd ../galexie/deploy && promtool test rules alerts_test.yml
