# raft-core

Production-oriented Raft protocol implementation in Go.

![Raft Core Protocol UI](https://github.com/user-attachments/assets/64765766-b1e2-4fa6-8e77-de33b8ec31b6)

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

Run node locally:

```bash
./bin/node --id=A --addr=:60051 --peers=B=localhost:60052,C=localhost:60053
```

## Contract

`proto/raft/raft.proto` is the source of truth for protocol wire compatibility.

## License

Add your preferred license file before public release.
