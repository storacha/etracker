help: ## Show this help
	@echo "Help"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-20s\033[93m %s\n", $$1, $$2}'

VERSION=$(shell awk -F'"' '/"version":/ {print $$4}' version.json)
DATE=$(shell date -u -Iseconds)
GOFLAGS=-ldflags="-X github.com/storacha/etracker/internal/build.version=$(VERSION) -X github.com/storacha/etracker/internal/build.Date=$(DATE) -X github.com/storacha/etracker/internal/build.BuiltBy=make"
TAGS?=

.PHONY: all build etracker test clean

all: build ## Make all targets

build: etracker client ## Build service and client binaries

etracker: ## Build the service binary
	go build $(GOFLAGS) $(TAGS) -o ./etracker github.com/storacha/etracker/cmd/etracker

client: ## Build the client binary
	go build $(GOFLAGS) $(TAGS) -o ./client github.com/storacha/etracker/cmd/client

test: ## Run tests
	go test -v ./...

clean: ## Clean up artifacts
	rm -f ./etracker
	rm -f ./client
