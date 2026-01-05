# An Explainable Distributed Storage Core (Reference Implementation)

> **A minimal, runnable distributed storage core for systems engineers**

---

## What is this?

This project is a **reference implementation of a distributed storage core**, designed to answer a fundamental but often obscured question:

> **What is the minimal set of mechanisms required to build a correct distributed storage system?**

Instead of chasing scale or features, this project focuses on:

- Correctness
- Explainability
- Runnable, verifiable end-to-end behavior

It is meant to be **read, debugged, and modified**, not deployed to production.

---

## Why does this exist?

Most real-world storage systems (Ceph, TiKV, HDFS):

- Are large and difficult to reason about
- Bury correctness logic under engineering complexity
- Rarely explain *why* certain mechanisms are necessary

Meanwhile, many educational demos:

- Ignore real crash and failure semantics
- Cannot be meaningfully exercised or validated

This project sits in between:

> **A minimal but real distributed storage core.**

---

## Project Scope

This is:

- ✔ A reference implementation of core storage mechanisms
- ✔ A system that can crash, fail over, and recover
- ✔ A codebase that supports building correct mental models

This is **not**:

- ❌ A production-ready storage system
- ❌ A full-featured KV or object store
- ❌ A performance-optimized solution

---

## Core Design Principles

### 1. Separate crash consistency from distributed consistency

- Local node:
  - WAL ensures crash correctness
- Cluster:
  - Primary–Backup replication with epoch fencing prevents split-brain

These concerns are **modeled independently** on purpose.

### 2. Explicit sources of truth

- During crash recovery: WAL is the sole source of truth
- At cluster level: epoch defines write legitimacy

There are no hidden states or implicit recovery paths.

### 3. Explicitly bounded scope

Advanced mechanisms (Raft, compaction, GC) are **intentionally deferred**  
to keep causal relationships clear.

---

## System Architecture

```
Client
  |
  v
Gateway (routing / retry / failover trigger)
  |
  v
Primary StorageNode  <---->  Backup StorageNodes
  |
  v
DiskEngine (WAL + Allocator + Data Records)
```

---

## DiskEngine: Local Storage Core

DiskEngine is a standalone C-based engine that demonstrates:

- Explicit on-disk layout
- WAL-driven crash consistency
- Verifiable recovery behavior

Key aspects:

- A/B superblocks with epoch
- WAL as the only recovery truth
- Immutable data records with CRC validation

---

## Dataplane: Minimal Distributed Write Path

The dataplane provides:

- Clear primary / backup roles
- Ordered replication
- Epoch fencing against split-brain
- Explicit failover on primary crash

The goal is to illustrate **a minimal yet realistic distributed write path**, not peak throughput.

---

## How to Use This Project

You can:

- Read the code to understand every state transition
- Kill the primary node and observe failover behavior
- Extend the system with checkpointing, GC, or consensus

You should not:

- Deploy this system to production
- Compare it to mature storage products by benchmarks

---

## Project Maturity

This is a **completed reference-grade implementation**:

- End-to-end data path is closed
- Crash and failover semantics are consistent
- Architectural boundaries are explicit

Future work is **evolutionary, not corrective**.

---

## Who Is This For?

- Distributed systems and storage engineers
- Learners who value correctness-first mental models
- Researchers seeking a runnable storage core reference
