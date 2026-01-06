# DiskEngine + Dataplane  
*A runnable distributed storage prototype with a real disk engine*

---

## 项目整体说明

本项目是一个**已经可以运行的分布式存储系统原型（MVP）**，由两个**职责严格划分、但协同工作的模块**组成：

- **Disk Engine（C）**  
  一个单机持久化内核，负责**磁盘布局、WAL、allocator、crash recovery**  
- **Dataplane（Go）**  
  一个网络数据平面，负责**RPC、进程解耦与请求路径编排**

两者共同组成一个**完整的数据闭环系统**：  
**Client → 网络 → 进程 → 磁盘 → crash → 恢复**

本 README 同时描述 **Disk Engine 与 Dataplane 的已实现行为**，  
不包含任何未落地的分布式机制或未来规划。

---

## 整体运行拓扑（事实）

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

- 一个 StorageNode **绑定一个 Disk Engine 实例**
- Dataplane 与 Disk Engine 之间通过 **明确的 cgo 边界**
- 当前以**本地多进程**方式运行，便于调试与 crash 验证

---

## Disk Engine（C）

Disk Engine 是系统的**持久化内核**，负责所有与磁盘一致性相关的工作。

### 职责范围

Disk Engine **只关心单机持久化正确性**：

- 物理空间管理（range allocator）
- WAL（redo-only）
- data region（blob / record 布局）
- crash recovery（WAL replay）
- 数据完整性校验（CRC）

Disk Engine **完全不感知**：

- 网络
- RPC
- 节点角色
- 分布式一致性

---

### On-Disk 结构（已实现）

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

- WAL 只描述**逻辑变更**
- Data Region 保存**最终数据形态**
- allocator 负责管理 data region 的物理空间

---

### 写入模型与提交点（关键细节）

一次 `disk_engine_put(key, value)` 的实际顺序为：

```
1. allocator_alloc()
2. write blob record to data region
3. fsync(data fd)
4. wal_append(BLOB_PUT)
5. wal_sync()   <-- 唯一提交点
```

**重要不变量：**

- WAL sync 之前的数据 **对外不可见**
- WAL sync 之后的数据 **必须在 crash 后可恢复**

---

### Crash Recovery 行为（可验证）

- crash 后：
  - 不扫描 data region
  - 仅 replay WAL
- replay 过程中：
  - 重建 allocator 状态
  - 重建 key → blob 映射
- 未 commit 的 data 写入会被自然丢弃

---

### 读路径（已实现）

```
disk_engine_get(key)
  -> lookup blob metadata
  -> read data region
  -> header CRC + payload CRC 校验
```

- 读路径 **不依赖 WAL**
- 正确性完全由 data region + 校验保证

---

## Dataplane（Go）

Dataplane 是 Disk Engine 之上的**最小网络数据平面**，其目标是：

> 把一个 crash-safe 的单机引擎，  
> 变成一个可以通过网络访问的服务。

---

### Dataplane 目录结构（运行相关）

```
dataplane/
├── cmd/
│   ├── gateway/        # Client 入口
│   └── storagenode/   # 数据节点
├── internal/
│   ├── ckv/            # Disk Engine 的 cgo 封装
│   └── rpc/pb/         # Gateway <-> StorageNode gRPC
└── scripts/
```

---

## StorageNode

StorageNode 是 **Disk Engine 的进程化封装**。

### 职责

- 管理一个 Disk Engine 实例的生命周期
- 将 RPC 请求 **同步映射** 为 disk_engine_* 调用
- 不缓存、不复制、不合并请求

### 写路径（逐步、与代码对齐）

一次 `Put(key, value)` 的完整执行路径为：

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

- crash 语义完全由 Disk Engine 决定
- StorageNode 不引入任何额外状态

---

## Gateway

Gateway 是 **Client 的唯一入口**。

### 行为事实

- Gateway **不访问磁盘**
- Gateway **不维护任何数据状态**
- 当前实现仅做：
  - gRPC API 暴露
  - RPC 参数透传
  - 请求转发

Gateway 的存在意义在于：

- 明确网络边界
- 将 RPC 复杂性与磁盘复杂性彻底隔离

---

## 系统当前能力边界（工程事实）

### 已实现并可运行

- crash-safe Disk Engine
- redo-only WAL + replay
- allocator + blob on-disk layout
- Gateway / StorageNode / Disk Engine 完整调用链
- 本地多进程 cluster 启动

### 刻意未实现

- WAL checkpoint / truncate
- allocator GC
- 副本复制 / 一致性协议
- shard / 自动路由

这些能力未实现，是为了保证：
> **所有已有路径都可单步调试、可 crash 验证。**

---

## 构建与运行

```bash
rm -rf build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

cd dataplane
./scripts/run_cluster.sh start
```

---

## 项目总结

这是一个**以 Disk Engine 为核心、由 Dataplane 组成完整系统的存储原型**：

- Disk Engine 解决“最难做对的事”：持久化与 crash consistency
- Dataplane 解决“最容易被写乱的事”：边界与调用路径
- 两者组合，形成一个真实可运行的分布式存储最小形态
