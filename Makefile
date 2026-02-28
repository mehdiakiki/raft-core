SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

.DEFAULT_GOAL := help

.PHONY: help doctor build proto test test-unit test-integration lint

PROTO_SRC := proto/raft/raft.proto
PROTO_OUT := gen/raft
GOPATH_BIN := $(shell go env GOPATH)/bin
GO_ENV := GOCACHE=$(CURDIR)/.cache/go-build

## doctor: verify core toolchain
doctor:
	@echo "Checking required tools..."
	@for tool in go protoc; do \
		if ! command -v $$tool >/dev/null 2>&1; then \
			echo "error: missing '$$tool'"; \
			exit 1; \
		fi; \
		echo "ok: $$tool -> $$(command -v $$tool)"; \
	done

## build: compile node binary
build:
	mkdir -p .cache/go-build bin
	$(GO_ENV) go build -o bin/node ./cmd/node

## proto: regenerate protobuf Go stubs
proto:
	mkdir -p $(PROTO_OUT)
	PATH="$(GOPATH_BIN):$$PATH" protoc \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		-I proto/raft \
		$(PROTO_SRC)

## test: run all tests
test: test-unit test-integration

## test-unit: run raft/storage unit tests
test-unit:
	mkdir -p .cache/go-build
	$(GO_ENV) go test -v -race -count=1 ./internal/raft/... ./internal/storage/...

## test-integration: run integration tests
test-integration:
	mkdir -p .cache/go-build
	$(GO_ENV) go test -v -race -count=1 -timeout=60s ./test/...

## lint: run go vet
lint:
	mkdir -p .cache/go-build
	$(GO_ENV) go vet ./...

## help: list available targets
help:
	@grep -E '^## ' Makefile | sed 's/## //'
