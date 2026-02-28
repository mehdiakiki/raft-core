# raft-core

Production-oriented Raft protocol implementation in Go.

<p align="center">
  <img src="https://github.com/user-attachments/assets/64765766-b1e2-4fa6-8e77-de33b8ec31b6" alt="Raft Core Protocol UI" width="760" />
</p>

This repository contains only consensus protocol code and protocol tests:

- Leader election (`RequestVote`, `PreVote`)
- Log replication (`AppendEntries`)
- Snapshots and restore
- Membership roles (voter/non-voter)
- ReadIndex linearizable reads
- Exactly-once client session handling
- Protocol metrics and state exposure

## Layout

- `cmd/node` — Raft node binary
- `internal/raft` — core consensus implementation
- `internal/storage` — storage backends (memory/bolt)
- `proto/raft` — protobuf contract
- `gen/raft` — generated Go stubs
- `test` — integration tests

## Quick start

```bash
make doctor
make build
make test
```

## Make targets

- `make doctor` - verify required toolchain (`go`, `protoc`).
- `make proto` - regenerate protobuf and gRPC Go stubs from `proto/raft/raft.proto`.
- `make build` - compile the node binary to `bin/node`.
- `make test` - run all tests (`test-unit` + `test-integration`).
- `make test-unit` - run unit tests for raft/storage with race detection.
- `make test-integration` - run integration tests with a timeout guard.
- `make lint` - run static checks via `go vet`.
- `make help` - list available targets.

Implementation note: Go targets use a local cache directory (`.cache/go-build`) for repeatable local/CI behavior.

Run node locally:

```bash
./bin/node --id=A --addr=:60051 --peers=B=localhost:60052,C=localhost:60053
```

## Contract

`proto/raft/raft.proto` is the source of truth for protocol wire compatibility.

## License

MIT. See [LICENSE](LICENSE).
