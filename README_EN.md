# DiskEngine + Dataplane  
*A runnable distributed storage prototype with a real disk engine*

---

## Project Overview

This project is a **runnable distributed storage system prototype (MVP)** composed of two
**strictly separated but cooperating modules**:

- **Disk Engine (C)**  
  A single-node persistence core responsible for **on-disk layout, WAL, allocation, and crash recovery**
- **Dataplane (Go)**  
  A network data plane responsible for **RPC, process isolation, and request orchestration**

Together, they form a **complete end-to-end data lifecycle**:  
**Client → Network → Process → Disk → Crash → Recovery**

This README describes **only behavior that is already implemented and runnable**.
No future features or unimplemented distributed mechanisms are assumed.

---

## Runtime Topology (Actual Behavior)

```
Client
  |
  | gRPC (Put / Get / Delete)
  v
Gateway (stateless)
  |
  | gRPC (forward)
  v
StorageNode (one process)
  |
  | cgo
  v
Disk Engine (one instance)
```

- Each StorageNode **owns exactly one Disk Engine instance**
- Dataplane and Disk Engine are separated by an explicit **cgo boundary**
- The system currently runs in **local multi-process mode**, optimized for debugging and crash testing

---

## Disk Engine (C)

The Disk Engine is the **persistence core** of the system.  
It is responsible for all disk-level correctness.

### Responsibility Scope

The Disk Engine handles **single-node persistence only**:

- Physical space management (range allocator)
- Write-ahead logging (redo-only)
- Data region layout (blob / record format)
- Crash recovery via WAL replay
- Data integrity verification (CRC)

The Disk Engine is **completely unaware of**:

- Networking
- RPC
- Node roles
- Distributed consistency

---

### On-Disk Layout (Implemented)

```
+------------------+
| Superblock       |
+------------------+
| WAL Region       |  (append-only, redo log)
+------------------+
| Data Region      |  (blob records)
+------------------+
| Free Space       |
+------------------+
```

- WAL records describe **logical state changes**
- The data region stores the **final physical data**
- The allocator manages physical space inside the data region

---

### Write Model and Commit Point (Key Details)

A single `disk_engine_put(key, value)` executes in the following order:

```
1. allocator_alloc()
2. write blob record to data region
3. fsync(data fd)
4. wal_append(BLOB_PUT)
5. wal_sync()   <-- the only commit point
```

**Key invariants:**

- Data written before WAL sync is **not externally visible**
- Data after WAL sync **must be recoverable after a crash**

---

### Crash Recovery Behavior (Verifiable)

- After a crash:
  - The data region is **not scanned**
  - Only the WAL is replayed
- WAL replay:
  - Reconstructs allocator state
  - Rebuilds key → blob mappings
- Uncommitted data writes are naturally discarded

---

### Read Path (Implemented)

```
disk_engine_get(key)
  -> lookup blob metadata
  -> read data region
  -> header CRC + payload CRC verification
```

- Reads **do not depend on the WAL**
- Correctness is guaranteed solely by the data region and checksums

---

## Dataplane (Go)

The Dataplane is the **minimal network data plane** built on top of the Disk Engine.

Its sole purpose is to:

> Expose a crash-safe single-node engine  
> as a network-accessible service.

---

### Dataplane Directory Structure (Runtime-Relevant)

```
dataplane/
├── cmd/
│   ├── gateway/        # Client entry point
│   └── storagenode/   # Data node
├── internal/
│   ├── ckv/            # cgo bindings for Disk Engine
│   └── rpc/pb/         # Gateway <-> StorageNode gRPC
└── scripts/
```

---

## StorageNode

The StorageNode is a **process-level wrapper around the Disk Engine**.

### Responsibilities

- Owns and manages a Disk Engine instance
- Maps RPC requests **synchronously** to `disk_engine_*` calls
- Performs no caching, replication, or request merging

### Write Path (Step-by-Step, Code-Aligned)

A single `Put(key, value)` follows this exact execution path:

```
Client
  -> Gateway.Put
    -> StorageNode.Put
      -> ckv.Put
        -> disk_engine_put
          -> allocator_alloc
          -> write data
          -> fsync
          -> wal_append
          -> wal_sync
```

- Crash semantics are fully determined by the Disk Engine
- StorageNode introduces no additional persistent state

---

## Gateway

The Gateway is the **only client-facing entry point**.

### Actual Behavior

- The Gateway **never accesses disk**
- The Gateway **maintains no data state**
- Its responsibilities are strictly limited to:
  - gRPC API exposure
  - Parameter forwarding
  - Request routing

The Gateway exists to:

- Establish a clear network boundary
- Isolate RPC complexity from disk complexity

---

## Current System Capabilities (Engineering Facts)

### Implemented and Runnable

- Crash-safe Disk Engine
- Redo-only WAL with replay
- Allocator + blob-based on-disk layout
- Full Gateway → StorageNode → Disk Engine request path
- Local multi-process cluster startup

### Intentionally Not Implemented

- WAL checkpointing / truncation
- Allocator garbage collection
- Replication or consensus protocols
- Sharding or automatic routing

These are excluded to ensure:

> **Every existing code path is debuggable and crash-verifiable.**

---

## Build and Run

```bash
rm -rf build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

cd dataplane
./scripts/run_cluster.sh start
```

---

## Project Summary

This project is a **storage system prototype centered on a real disk engine**:

- The Disk Engine solves the hardest problem correctly: persistence and crash consistency
- The Dataplane solves the most error-prone problem cleanly: boundaries and execution paths
- Together, they form a real, runnable minimal distributed storage system
