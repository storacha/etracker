help: ## Show this help
	@echo "Help"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[36m%-20s\033[93m %s\n", $$1, $$2}'

VERSION=$(shell awk -F'"' '/"version":/ {print $$4}' version.json)
COMMIT=$(shell git rev-parse --short HEAD)
DATE=$(shell date -u -Iseconds)
GOFLAGS=-ldflags="-X github.com/storacha/piri/pkg/build.version=$(VERSION) -X github.com/storacha/piri/pkg/build.Commit=$(COMMIT) -X github.com/storacha/piri/pkg/build.Date=$(DATE) -X github.com/storacha/piri/pkg/build.BuiltBy=make"
TAGS?=

.PHONY: all build payme test clean

all: build ## Make all targets

build: payme ## Build the service binary

payme: ## Build the service binary
	go build $(GOFLAGS) $(TAGS) -o ./payme github.com/storacha/payme/cmd

test: ## Run tests
	go test -v ./...

clean: ## Clean up artifacts
	rm -f ./payme
