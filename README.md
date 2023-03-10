# rs-raft

This is an implementation of Raft consensus algorithm
referring [the respected paper](https://raft.github.io/raft.pdf).

## How to run

* Run process

```
RUST_LOG=info RAFT_RPC_PORT=8090 OTHER_NODES=localhost:8070,localhost:8080 cargo run
```

* Run tests

```
RUST_LOG=info cargo test
```

## Concept

* This implementation is inspired from the Kubernetes reconcile loop.
    * `raft_reconciler.rs` has a loop which reconciles rpc and data periodically according to the
      Raft state.

* Each Raft process connects with others via gRPC.
    * `raft_server.rs` is the server and `raft_client.rs` is the client implementation.

* All the business logic of the consensus algorithm is placed in `raft_state.rs`.

## Not implemented features

* State snapshot
* Cluster membership changes