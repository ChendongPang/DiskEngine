# DiskEngine + Dataplane

A **runnable distributed storage prototype** consisting of two parts:

- **Disk Engine**: a C/C++ storage engine responsible for persistence and crash consistency
- **Dataplane**: a Go-based data plane implementing replication, failover, and recovery

This project focuses strictly on **mechanisms that are already implemented and runnable**.
It does not include unimplemented features or advanced consensus protocols such as Raft.

---

## Architecture Overview

The dataplane implements a **Primary–Backup replication model** with:

- ShardView and epoch-based split-brain prevention
- A MetaCoordinator as the authoritative control plane
- Gateway-driven routing and failover
- StorageNodes with oplog-based replication and recovery
- Prepare / Commit logging and replay / repair

---

## Components

### Disk Engine (C/C++)

The Disk Engine provides:

- Low-level persistence (allocator, blobs, WAL)
- Redo-only logging for crash consistency
- Recovery solely via WAL replay (no data region scan)

The Disk Engine itself contains **no distributed logic**.
All replication and consistency are handled by the dataplane.

---

### Dataplane (Go)

The dataplane links against the disk engine via cgo and consists of:

#### MetaCoordinator (Control Plane)

- Maintains authoritative **ShardView** for each shard
- ShardView includes: `epoch`, `primary`, `backups`, and `write quorum`
- Uses CAS (Compare-And-Swap) to advance epoch safely during failover
- Acts as the **single source of truth** for shard configuration

#### Gateway

Gateway is the single client entry point and is responsible for:

- Exposing gRPC APIs
- Caching and refreshing ShardView
- Triggering failover on write failures
- Selecting a new primary based on `maxCommitted`
- Pushing new epoch and role configuration to StorageNodes

Gateway does not access disk and does not store user data.

#### StorageNode

StorageNodes act as **Primary or Backup** replicas:

- All writes go through an oplog (Prepare / Commit)
- The primary replicates oplog entries to backups
- Backups commit and apply entries in sequence order
- Crash recovery is handled via oplog replay
- After failover, a repair process reconciles replica state

StorageNodes do not introduce additional **persistent semantics**;
crash consistency remains entirely within the Disk Engine.

---

## Build & Run

This project uses a **two-stage build**:

1. Disk Engine (C/C++, via CMake)
2. Dataplane (Go, linked via cgo)

### Prerequisites

- Linux (x86_64)
- Go **>= 1.22**
- CMake >= 3.16
- GCC / Clang
- Make or Ninja

### 1. Build Disk Engine

```bash
rm -rf build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 2. Build Dataplane

```bash
cd dataplane
go build ./cmd/metacoordinator
go build ./cmd/gateway
go build ./cmd/storagenode
go build ./cmd/client
```

### 3. Run a Local Demo Cluster

```bash
cd dataplane
./scripts/run_cluster.sh start
```

### 4. Verify

```bash
./client -addr 127.0.0.1:9200 put k1 v1
./client -addr 127.0.0.1:9200 get k1
./client -addr 127.0.0.1:9200 del k1
```
