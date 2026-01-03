# DiskEngine —— 磁盘结构与数据路径说明

DiskEngine 是一个**最小但完整的磁盘引擎（Disk Engine）实现**，
采用 **allocator-first + WAL（Write-Ahead Logging）** 的设计，
提供 **可恢复（crash-consistent）** 的数据写入与读取能力。

本 README 仅描述**当前已经实现的事实**，不包含未来规划或扩展设计。

---

## 1. 整体设计概览

DiskEngine 的核心目标是：

- 将 **payload 数据** 安全写入磁盘
- 通过 **WAL** 保证崩溃后一致性恢复
- 明确区分 **数据存储（Data Region）** 与 **元数据日志（WAL）**
- 保持结构简单、语义可推导

DiskEngine **不是文件系统，也不是数据库**，而是一个底层磁盘引擎。

---

## 2. 磁盘物理结构（On-Disk Layout）

DiskEngine 使用 **单一磁盘文件 / block device（一个 fd）**，
通过 **固定 offset 划分磁盘区域**，在物理层面分离不同职责。

### 2.1 整体磁盘布局

```
Disk / File (fd)
offset →
┌──────────────────────────────────────────────────────────────────────────────┐
│ Superblock A │ Superblock B │              WAL Region              │  Data   │
│     4KB      │     4KB      │          wal_len (固定大小)           │ Region  │
├──────────────┴──────────────┴─────────────────────────────────────┴──────────┤
0              4K             8K                                data_off
```

关键字段（存储在 superblock 中）：

```
sb.wal_off   = 2 * 4KB
sb.wal_len   = 固定大小（例如 4MB）
sb.data_off  = sb.wal_off + sb.wal_len
sb.data_len  = 设备大小 - sb.data_off
```

**WAL 与 Data 在物理上通过 offset 严格分离，不可能发生重叠。**

---

## 3. Superblock（A/B 双写）

Superblock 采用 **A/B 双写 + epoch + CRC 校验**，用于防止部分写损坏。

```
Superblock Slot A @ offset 0
┌──────────────────────────────────────────────────────────┐
│ magic | version | epoch | wal_off | wal_len | data_off   │
│ data_len | alloc_unit | wal_ckpt_lsn | crc32 | ...       │
└──────────────────────────────────────────────────────────┘

Superblock Slot B @ offset 4KB
┌──────────────────────────────────────────────────────────┐
│ 相同结构，epoch 更新                                      │
└──────────────────────────────────────────────────────────┘
```

启动时的选择规则：

```
选择 crc 校验通过 且 epoch 最大 的 superblock
```

---

## 4. WAL 区域（Write-Ahead Log）

WAL 是一个 **固定大小窗口内的顺序追加日志**，
只用于 **崩溃恢复（crash recovery）**，不参与正常读路径。

### 4.1 WAL 区域布局

```
WAL Region [wal_off .. wal_off + wal_len)

wal_off
  ↓
  ┌──────────────┬──────────────┬──────────────┬──────────────┐
  │ WAL Record 1 │ WAL Record 2 │ WAL Record 3 │     ...      │
  └──────────────┴──────────────┴──────────────┴──────────────┘
                                              ↑
                                           wal.write_off
```

### 4.2 WAL 记录格式

```
┌──────────────────── wal_rec_hdr_t ──────────────────────┐
│ magic | version | type | header_sz | payload_len         │
│ lsn | seq | crc32 | record_total_len                      │
└──────────────────────────────────────────────────────────┘
┌──────────────────────── payload ─────────────────────────┐
│ 例如 BLOB_PUT: key + blob_id + data_off + data_len        │
└──────────────────────────────────────────────────────────┘
┌──────────── 对齐 padding（可选） ─────────────┐
```

---

## 5. Data Region（数据区）

Data Region 是一整块连续空间，由 allocator 统一管理。

```
Data Region [data_off .. data_off + data_len)

data_off
  ↓
  ┌────────────────┬───────────────┬────────────────┬──────────────┐
  │ blob record A  │   free space   │ blob record B  │  free space  │
  └────────────────┴───────────────┴────────────────┴──────────────┘
```

### 5.1 Blob Record 物理格式

```
rec_off
  ↓
  ┌────────────── blob_record_hdr_t ───────────────┐
  │ magic | version | payload_len                  │
  │ header_crc32 | payload_crc32 | ...              │
  └────────────────────────────────────────────────┘
  ┌──────────────────── payload bytes ─────────────┐
  │ 用户数据                                        │
  └────────────────────────────────────────────────┘
```

---

## 6. 数据路径（Data Path）

### 6.1 写路径（disk_engine_put）

```
disk_engine_put(key, data, len)
│
│ 1) ra_alloc() → rec_off
│
│ 2) 写 payload @ rec_off + header
│
│ 3) 写 blob header @ rec_off
│
│ 4) fdatasync()        （数据先持久化）
│
│ 5) wal_append(BLOB_PUT)
│
│ 6) wal_sync()         （提交点）
│
│ 7) index[key] = { rec_off, len }
```

---

## 7. 核心不变量（Invariants）

1. **WAL 与 Data 通过固定 offset 物理隔离**
2. **payload 永远先于 WAL 提交落盘**
3. **WAL 是恢复阶段的唯一事实来源**
4. **稳态读路径不依赖 WAL**
