# DiskEngine

DiskEngine is a minimal, allocator-first disk engine inspired by
Ceph BlueStore. It focuses on crash consistency, predictable I/O,
and explicit control over on-disk layout.

## Design Goals

- Allocator-first disk layout (no traditional filesystem)
- WAL-based crash consistency
- Explicit on-disk format (superblock A/B + epoch)
- Range-based physical allocation
- Blob-based data semantics
- Minimal abstractions, auditable code

## Non-Goals

- POSIX filesystem semantics
- Distributed metadata or networking
- User-facing object namespace

## Architecture Overview

