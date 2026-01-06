# DiskEngine + Dataplane

一个**可运行的分布式存储原型**，由两部分组成：

- **Disk Engine**：使用 C/C++ 实现的底层磁盘引擎，负责数据与元数据的持久化与 crash consistency
- **Dataplane**：使用 Go 实现的数据平面，基于 Primary–Backup 模型提供复制、failover 与恢复能力

本项目专注于**已经落地并可运行的机制**，不包含尚未实现的高级一致性协议（如 Raft）或未来规划。

---

## 架构概览

Dataplane 采用 **Primary–Backup** 复制模型，核心机制包括：

- ShardView + epoch（防脑裂）
- MetaCoordinator 作为 shard 视图的权威控制面
- Gateway 负责路由、failover 触发与配置下发
- StorageNode 负责数据读写、oplog 复制与恢复
- 基于 oplog 的 Prepare / Commit 两阶段写入与 replay / repair

---

## 组件说明

### Disk Engine（C/C++）

Disk Engine 负责：

- allocator / blob / WAL 等底层持久化机制
- crash-safe 的 redo-only 日志
- 重启时仅依赖 WAL 进行恢复（不扫描 data region）

Disk Engine 本身**不包含分布式逻辑**，所有副本复制与一致性由 Dataplane 实现。

---

### Dataplane（Go）

Dataplane 通过 cgo 链接 Disk Engine，并实现以下组件：

#### MetaCoordinator（控制面）

- 维护每个 shard 的 **ShardView**
- ShardView 包含：`epoch / primary / backups / write quorum`
- 通过 CAS（Compare-And-Swap）推进 epoch，实现安全切主
- 是 shard 视图的**唯一权威来源**

#### Gateway

Gateway 是客户端的唯一入口，负责：

- 对外暴露 gRPC API
- 缓存并刷新 ShardView
- 写失败时触发 failover（通过 MetaCoordinator CAS）
- 选择新 primary（基于 maxCommitted）
- 将新 epoch 与角色配置下发给 StorageNode

Gateway 本身不访问磁盘，也不保存用户数据。

#### StorageNode

StorageNode 是数据节点，角色为 **Primary 或 Backup**：

- 所有写入通过 oplog（Prepare / Commit）保证顺序与一致性
- Primary 负责复制 oplog 到 backups
- Backup 按 seq 顺序提交并应用
- 崩溃重启时通过 oplog replay 恢复状态
- 切主后通过 repair 流程补齐副本状态

StorageNode 不引入额外的**持久化语义**，
所有 crash consistency 仍完全由 Disk Engine 决定。

---

## 构建与运行

本项目包含 **两阶段构建**：

1. Disk Engine（C/C++，CMake）
2. Dataplane（Go，通过 cgo 链接 Disk Engine）

### 环境依赖

- Linux（x86_64）
- Go **>= 1.22**
- CMake >= 3.16
- GCC / Clang
- Make 或 Ninja

### 1. 构建 Disk Engine（C/C++）

```bash
rm -rf build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

### 2. 构建 Dataplane（Go）

```bash
cd dataplane
bash scripts/gen_proto.sh
go build ./cmd/metacoordinator
go build ./cmd/gateway
go build ./cmd/storagenode
go build ./cmd/client
```

### 3. 运行本地 Demo 集群

```bash
cd dataplane
./scripts/run_cluster.sh start
```

### 4. 简单验证

```bash
./client -addr 127.0.0.1:8000 put k1 v1
./client -addr 127.0.0.1:8000 get k1
./client -addr 127.0.0.1:8000 del k1
```
