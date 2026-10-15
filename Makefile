# The lab CLI (cli/), the worker (worker/), the psql host's Caddy build
# (psql/caddy) and the API's TypeScript types (generated/). `make dist` builds
# what is copied to the servers, all linux/amd64 and stamped with one VERSION:
# bin/lab for both hosts, bin/worker for the galexie host, bin/caddy for the
# psql host.
GO      ?= go
VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
LINUX   := CGO_ENABLED=0 GOOS=linux GOARCH=amd64

.PHONY: build test vet fmt check check-all dist types types-check clean

# lab for this machine (--help and the man page only: it runs on Ubuntu 24.04 amd64).
build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/lab-$(shell $(GO) env GOOS)-$(shell $(GO) env GOARCH) ./cli

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...
	GOOS=linux GOARCH=amd64 $(GO) vet ./...
	cd psql/caddy && $(GO) vet ./...

fmt:
	@out=$$(gofmt -l cli galexie psql dashboard clickhouse); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# The CLI and Caddy. The worker has its own `make -C worker check` (it needs
# TEST_DATABASE_URL); check-all runs both.
check: fmt vet test
	$(LINUX) $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o /dev/null ./cli
	cd psql/caddy && $(LINUX) $(GO) build -o /dev/null .

check-all: check
	$(MAKE) -C worker check

dist:
	$(LINUX) $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/lab ./cli
	$(MAKE) -C worker build-linux VERSION=$(VERSION)
	cp worker/bin/worker-linux-amd64 bin/worker
	cd psql/caddy && $(LINUX) $(GO) build -trimpath -ldflags '-s -w' -o ../../bin/caddy .
	cd bin && shasum -a 256 lab worker caddy

# generated/api.ts from a database with psql/api.sql applied (generated/README.md):
#   make types DATABASE_URL=postgres://localhost:5432/explorer
types:
	cd generated && npm ci --silent && DATABASE_URL='$(DATABASE_URL)' node gen.mjs > api.ts.new && mv api.ts.new api.ts

# Fails when generated/api.ts doesn't match the database's api schema.
types-check:
	cd generated && npm ci --silent && DATABASE_URL='$(DATABASE_URL)' node gen.mjs | diff -u api.ts - && echo "generated/api.ts is current"

clean:
	rm -rf bin worker/bin generated/node_modules
