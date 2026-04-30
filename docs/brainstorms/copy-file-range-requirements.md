# 需求：FUSE copy_file_range 元数据级快速拷贝

- **创建日期**：2026-04-30
- **状态**：Brainstorm 已完成，待进入 plan
- **范围**：Standard

## 1. 问题与目标

DingoFS 客户端目前未实现 FUSE 的 `copy_file_range` 操作（`src/client/fuse/fuse_lowlevel_ops_func.h:74` 当前为 `nullptr`），用户态 `cp` / 应用程序通过 `copy_file_range(2)` 拷贝大文件时会回退为内核读+写路径，需要把 src 的全部数据从对象存储读到客户端再写回去，开销巨大。

目标是利用 DingoFS 底层 slice/block **不可变**这一既有特性，在拷贝时**只复制元数据（slice 引用），不拷贝 block 数据**，让 GB/TB 级文件拷贝退化为 O(slice 数) 的元数据操作。

成功标准：
- 大文件拷贝耗时与文件大小**解耦**，主要正比于 slice 条数。
- 完全对齐 Linux `copy_file_range(2)` 语义：跨文件/同文件、任意 (src_off, dst_off, len)、返回实际拷贝字节数。
- 拷贝完成后 src 与 dst 互相独立：任意一方后续写入不可见于另一方。
- 不出现"slice 已被 GC，dst 读到坏数据"的悬挂引用。

非目标：
- 不实现 ioctl `FICLONE`/`FICLONERANGE`（如果将来要做，可复用同一套底层 RPC，本次不引入 ioctl 路径）。
- 不引入用户可见的快照 / clone 概念。
- 不优化 src/dst 跨文件系统（FUSE 已经会用 EOPNOTSUPP 走慢路径）。

## 2. 关键设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 拷贝粒度 | 范围级 + 同文件，对齐 Linux 全部语义 | 避免后续被迫扩 API；上层工具（GNU coreutils 9+）已经依赖完整语义 |
| CoW 机制 | **不需要**。slice/block 本来就不可变，新写入总是分配新 slice 和新 block | 与 DingoFS 现有写路径完全一致，无需引入 CoW 状态位 |
| 跨 inode slice 生命周期保护 | **方案 B：反向索引** `slice_id -> {(ino, chunk_idx)}` | 比 refcount 更灵活，GC/compact 路径只多一次反向查询，无需修改现有 Slice schema |
| src 脏数据一致性 | **客户端在发 RPC 前先 Flush src** 的脏页转成 slice，然后再发 CopyFileRange | 保证语义强；当前 VFS 层用 ino+fh 即可发起 flush（src 可能是 O_RDONLY 没有 dst 的 fh，需要支持按 ino flush） |
| dst 已有数据处理 | **追加新 slice**（指向 src 的 slice id）覆盖到 dst chunk slice 列表后端，旧 slice 由 compact 自然清理 | 与 `WriteSlice` 现有路径一致；反向索引会保护原 src slice 不被误删 |

## 3. 用户可见行为

### 3.1 接口

实现 FUSE low-level op：

```c
copy_file_range(src_ino, src_off, dst_ino, dst_off, len, flags) -> ssize_t
```

返回值：实际拷贝字节数（`<= len`），或 -errno。

### 3.2 行为细则

- **flags**：当前 Linux 内核未定义任何 flag，传入非 0 返回 `EINVAL`。
- **len 截断**：如果 src_off + len 超过 src 文件末尾，自动截断到文件末尾（与内核语义一致）。
- **同文件 + 区间重叠**：返回 `EINVAL`（对齐 Linux 5.3+ 行为）。
- **同文件不重叠**：允许，与跨文件路径同实现。
- **dst 文件大小**：拷贝结束后 dst 文件大小至少为 `max(原大小, dst_off + 实际拷贝字节)`，并更新 mtime/ctime；src 的 atime 按挂载选项决定。
- **权限**：src 需要可读、dst 需要可写；任一权限不足返回 `EACCES`。
- **目录/特殊文件**：非普通文件返回 `EISDIR`/`EINVAL`。
- **fs_id 不同**：本期不支持跨 fs_id 拷贝，返回 `EXDEV`，让内核走慢路径。
- **配额**：dst 文件逻辑大小增加，**计入 quota**（与 fallocate 一致）；底层 block 是共享的，不重复计入物理用量统计（这一行为需要在 plan 阶段与 quota 模块对齐）。

## 4. 端到端流程（高层视图）

```
fuse_op_copy_file_range
   └─ VFS::CopyFileRange(src_ino, src_off, dst_ino, dst_off, len, flags)
        ├─ 参数校验、权限校验、重叠检查
        ├─ Flush(src_ino, src_fh)               // 把 src 脏数据落成 slice
        ├─ for each chunk overlap in src:
        │     slices = MetaSystem::ReadSlice(src_ino, chunk_idx)
        │     裁剪 slices 到 [src_off, src_off+len) 范围
        │     映射到 dst chunk 坐标（调整 pos/off/len，按 dst chunk_size 切分）
        ├─ MetaSystem::CopyFileRange(src_ino, dst_ino, mapped_slices_per_chunk)
        │     → MDS RPC（一次 RPC 内多 chunk 的 delta_slices）
        └─ 返回实际拷贝字节数
```

MDS 端单次 RPC 内事务地完成：
1. 校验 src/dst 仍存在、未删除、文件类型为 File。
2. 对每个目标 chunk：append 新 slice 到 dst chunk slice 列表，bump version。
3. 对每个被引用的 src slice id：在反向索引 KV 中插入 `(slice_id, dst_ino, dst_chunk_idx)` 记录。
4. 更新 dst inode 的 size/mtime/ctime/length。

## 5. slice 反向索引（核心新增机制）

### 5.1 KV 结构

新增 keyspace：

```
key  : SliceRef | fs_id | slice_id | ref_ino | ref_chunk_idx
value: empty (或时间戳，便于 debug)
```

- `slice_id` 已经是全 fs 唯一（`AllocSliceIdRequest`）。
- 用前缀扫描 `SliceRef|fs_id|slice_id|*` 即可枚举 slice 当前所有 referrer。
- **隐含约定**：slice 的"原生 owner"（首次 `WriteSlice` 写入它的 inode）**也要登记**到反向索引中，否则跨拷贝后 owner 删除时无法判断 slice 还在被别人引用。这是本次必须新增的一致性要求。

### 5.2 GC / compact 路径修改

- `gc.cc` 中决定把 slice 写入 trash 之前，必须先反查反向索引：
  - 若 `(slice_id, ino, chunk_idx)` 是该 slice 的最后一个 referrer → 删除反向索引项 + 走原 trash 流程。
  - 否则 → 仅删除该 referrer 项，**不**写 trash、**不**删 block。
- `store_operation.cc:1983-2005`（compact 时筛选 trash slice）同样改造。
- unlink 路径：删除 inode 的所有 chunk 时，按上述规则逐 slice 处理。

### 5.3 一致性

- 所有"修改 inode 的 slice 列表" + "维护反向索引"必须在**同一事务**中完成，以避免 crash 后引用泄漏或悬挂。
- `CopyFileRange` 一次 RPC 内可能涉及多个 chunk，需要单次大事务（与现有 `WriteSlice` 多 chunk delta 的事务范围一致即可）。

## 6. 风险与待办

- **反向索引膨胀**：极端场景下一个 slice 被高频 reflink，索引项数量可能很大。本期接受，后续可加阈值告警。
- **历史 slice 兼容**：升级前已存在的 slice 没有 owner 反向索引。需要决定：(a) 升级时一次性扫描回填；(b) 升级后仅对**新写入**的 slice 维护反向索引，老 slice 走旧 GC 行为且禁止参与 reflink。倾向 (b)（成本低、风险小），plan 阶段确认。
- **src 在其他客户端有打开 fh**：本期客户端 Flush 只能 flush 自己进程内的脏页。其他客户端的脏页对本次拷贝不可见；这是 POSIX `copy_file_range` 允许的行为，但需要在文档中明确声明。
- **同文件重叠检测**：当 src_ino == dst_ino 时需要范围重叠判断；细节边界（dst_off == src_off + len 是否算重叠）按 Linux 内核现行行为实现。
- **配额一致性**：如果 dst 拷贝后逻辑大小增加但物理 block 共享，物理用量统计与 quota 计算需对齐（plan 阶段与 quota 模块负责人确认）。

## 7. 涉及的代码区域（仅指引，详细设计放到 plan）

- FUSE op：`src/client/fuse/fuse_op.h:1076`、`src/client/fuse/fuse_lowlevel_ops_func.h:74`
- VFS 接口：`src/client/vfs/vfs.h`（新增虚函数 `CopyFileRange`）、`src/client/vfs/vfs_impl.{h,cc}`、`src/client/vfs/vfs_wrapper.{h,cc}`
- MetaSystem 接口：`src/client/vfs/metasystem/meta_system.h`、各实现（`mds/`、`memory/`、`local/`）
- MDS 协议：`proto/dingofs/mds.proto`（新增 `CopyFileRange` RPC + 消息）
- MDS 服务实现：`src/mds/service/`、`src/mds/filesystem/`（新增 operation）、`src/mds/storage/`（反向索引 codec）
- GC / compact：`src/mds/background/gc.cc`、`src/mds/filesystem/store_operation.cc:1983-2005`
- 单元测试：`test/unit/mds/`、`test/unit/client/vfs/`

## 8. 进入 plan 前需 plan 阶段确认的问题

1. 老 slice 是否回填反向索引（推荐：不回填，禁止老 slice 参与 reflink）。
2. 配额对"逻辑大小 vs 物理用量"的口径。
3. 反向索引的 KV 编码细节、是否复用现有 `MetaCodec`。
4. 是否限制单次 `CopyFileRange` RPC 涉及的 chunk 数（避免巨大事务）。如果限制，客户端按 chunk 切分多次 RPC 调用，并保证每次 RPC 内自洽。

---

## 附录 A：反向索引深挖

### A.1 现状关键事实（已通过代码确认）

- **MDS 已经预留了 `SliceRef` keyspace**（`src/mds/common/codec.cc:124`、`src/mds/common/codec.h:148-153`）：
  - key：`{prefix} | kTableMeta | kMetaFsSliceRef(=29) | slice_id(8B)`，**全 fs 范围**（不带 fs_id；slice_id 由 `AllocSliceIdRequest` 全 fs 唯一分配）
  - value：`(size: u32, ref_count: u32)`
  - **代码搜索确认：除了 codec 本身，没有任何业务代码在用它**。这是一处冷启动时就预留好的 refcount-style 槽位。
- 当前 GC 是异步两阶段：`compact`/`unlink` 时把不再被该 inode 引用的 slice 写入 `DelSlice` keyspace（`store_operation.cc:2038`），后台 `gc.cc` 扫描，超过 `mds_gc_delslice_reserve_time_s`（默认 480s）后真删 S3/Rados block。
- `WriteSliceOperation` 是 BatchGet + 单 txn 多 chunk Put（`store_operation.cc:1101-1108`），没有硬性 chunk 数上限。compacted slice id 会保留在 `chunk.compacted_slices` 里做幂等去重。
- slice id 是不可变的全局唯一标识；blockstore 的 key 由 `EnumerateBlockKeys(slice_id, slice.size, block_size)` 推导，**只依赖 slice 元数据**，与 referrer 的 (ino, chunk_idx) 无关。

### A.2 选择重审：refcount vs 反向索引

| 维度 | 方案 A：refcount (复用已有 `SliceRef`) | 方案 B：反向索引（B 选定） |
|---|---|---|
| KV 项数 / slice | 1 | 1 + N（N = 引用 inode 数） |
| 增删 referrer 的并发冲突 | RMW 单行（同一 slice 高频 reflink 时强冲突） | 各 referrer 一行，无冲突 |
| GC 路径开销 | Get 1 row, dec | Prefix scan + 删自己一行 |
| 排查 / 审计 | 只知道"还有几个用" | 能列出**谁**在用（运维/调试价值大） |
| crash 后修复 | 没法定位悬挂引用 | 可以扫描验证 referrer 指向是否真实存在 |
| 复用现有 codec | ✅ 直接用 | 需新增 keyspace |

**结论**：维持方案 B。但**不复用** `SliceRef` 现有字段语义——它是 refcount 设计；新增一组 keyspace `SliceReferrer` 与之独立。是否同时也把 `SliceRef` (size, ref_count) 写起来作为快速判断的"短路"路径，留到 plan 阶段权衡（额外 RMW vs 一次 prefix scan 的性能）。

### A.3 KV 编码方案候选

#### 候选 B1（推荐）：拍扁式，每 referrer 一行

```
key   : {prefix} | kTableMeta | kMetaFsSliceReferrer(新增) | slice_id(8B, big-endian) | ref_fs_id(4B) | ref_ino(8B) | ref_chunk_idx(8B)
value : 空 或 add_time_ns(8B)
```

- 编码全 fs 共享前缀（与 `SliceRef` 同位置），但 key 中带 ref_fs_id 以支持后续跨 fs 场景。
- "该 slice 还有没有 referrer" = `txn->Scan({prefix}|kTableMeta|kSliceReferrer|slice_id, limit=1)` 是否有结果。
- 增删都是单行 Put/Delete，**无 RMW**，事务冲突最小。
- key 顺序 (slice_id, ino, chunk) 让 prefix scan O(返回行数)。

#### 候选 B2：聚合式，每 slice 一行 list

```
key   : ... | slice_id
value : repeated SliceReferrer{ ref_fs_id, ref_ino, ref_chunk_idx, add_time_ns }
```

- 单点 Get，但每次增删都要 RMW 整个 list；同一 slice 高频 reflink 时事务冲突高，不推荐。

#### 候选 B3：refcount + 稀疏 list 混合

放弃。失去了"能精确定位悬挂引用"的核心优势。

**采用 B1。**

### A.4 事务边界

#### A.4.1 `WriteSlice`（**改造现有路径**）

每当 `WriteSliceOperation` 第一次把一个 slice 加入某 inode 的 chunk（即 `is_exist_fn` 返回 false 的分支），**在同一 txn 内**追加：

```
Put(SliceReferrer{slice_id, fs_id, ino, chunk_index}, now_ns)
```

这是"原生 owner 登记"。等价于：**任何持有该 slice 的 inode/chunk 都必须在 SliceReferrer 中有对应行**，没有 owner 与 reflink 用户的区别。后续 compact/unlink 用同样规则处理。

成本：每个**新写入**的 slice 多 1 个 KV Put。对于一次包含 N 个新 slice 的 WriteSlice RPC：从 N 个 chunk Put 增加到 N + N' 个 Put（N' 是新 slice 数，通常 ≈ N）。可接受。

#### A.4.2 `CopyFileRange`（新增 RPC）

单 RPC 单 txn，包含：
1. `BatchGet`：dst 涉及的所有 chunk + dst inode +（视方案）src inode/chunk 校验项。
2. **冲突检测读**：对每个 src slice id，在同一 txn 内 `Get(SliceReferrer{slice_id, src_fs_id, src_ino, src_chunk_idx})`，若不存在 → 说明 src 在并发 compact/unlink 把它带走了，整个 RPC 返回失败让客户端重试。SnapshotIsolation 也会作为兜底。
3. 对每个目标 dst chunk：`Put(ChunkEntry)`（append 新 slice 到 slices 列表）。
4. 对每个被引用的 src slice id：`Put(SliceReferrer{slice_id, dst_fs_id, dst_ino, dst_chunk_idx}, now_ns)`。
5. `Put(InodeEntry dst)`：更新 size/mtime/ctime。

**事务大小估算**：100 GB 文件、64 MB chunk → 1600 chunks。假设每 chunk 平均 2 slice → 1600 chunk Put + 3200 referrer Put + 1 inode Put ≈ 4800 ops。这是大事务。

**应对**：plan 阶段确认是否需要客户端按 chunk 范围切分多次 RPC。建议引入 `mds_copy_file_range_max_chunks_per_rpc`（默认例如 256），客户端按窗口循环调用，每次窗口内自洽。

#### A.4.3 `Compact` / `Unlink` 路径（**改造现有路径**）

原逻辑：把不再被本 inode 引用的 slice 直接写入 DelSlice 队列（`store_operation.cc:1983-2005`）。

改造（**全部在原 compact/unlink txn 内完成**）：

```
for each slice s being removed from this inode's chunk:
    Delete(SliceReferrer{s.id, fs_id, ino, chunk_idx})    # 删除自己这一行
    if Scan(SliceReferrer{s.id, *}, limit=1) returns empty:
        # 真没人用了
        加入 trash_slice_list -> Put(DelSlice...)           # 走原 GC 路径
    else:
        # 还有别人在用，不进 trash
        skip
```

成本：每个被删除的 slice 多 1 个 Delete + 1 个 limit=1 的 Scan。Scan 在 LSM/distributed KV 上是定位 + 1 次 next，开销低。可接受。

#### A.4.4 GC 后台扫描兼容

`gc.cc:CleanDelSlice` 拿到 trash 项后直接删 block。改造后：
- **进入 trash 的前提已经是"无 referrer"**，所以 GC 后台逻辑**不需要再次校验**——只要保证"写 trash" 与"删最后一个 SliceReferrer"是同一个 txn 即可。
- 但仍建议在 GC 真删 block 前再做一次 `Scan(SliceReferrer{slice_id, *}, limit=1)` 兜底（开销低、防 bug），失败则跳过本批次本项并告警。

### A.5 历史 slice 兼容（升级路径）

老 slice 在 SliceReferrer 里没有任何行。两种策略：

| 方案 | 描述 | 取舍 |
|---|---|---|
| 严格回填 | 升级时一次性扫描全 fs 所有 inode chunk，回填 owner referrer | 重；大集群可能扫描数小时；但语义干净 |
| **宽松（推荐）** | 不回填；引入 `slice_id` 阈值 `min_referrer_slice_id`（升级时记录当前 max slice_id）。compact/unlink 路径里：`if slice_id <= min_referrer_slice_id → 走旧逻辑直接 trash, 不查 SliceReferrer`；CopyFileRange 路径里：`if 任一 src slice_id <= min_referrer_slice_id → 拒绝（EOPNOTSUPP）或 fallback` | 实现简单；老文件无法 reflink，但用户可以用一次普通 `cp` 重写后再 reflink |

推荐宽松方案。`min_referrer_slice_id` 持久化到 fs 级元数据。

### A.6 性能模型

| 路径 | 增量开销 |
|---|---|
| `WriteSlice` (新写入) | +1 KV Put per 新 slice（无 Get）|
| `CopyFileRange` | 主开销 = O(涉及 chunk 数 + 引用 slice 数) 个 KV op，**与文件大小无关** |
| `Compact` | +1 Delete + 1 limit=1 Scan per 被删 slice |
| `Unlink` 大文件 | 同 Compact，每 slice + 2 ops。1600-chunk 大文件 ≈ +3200 ops |
| GC 后台 | 可选 1 个 limit=1 Scan per slice（兜底） |

CopyFileRange 100 GB 文件实测目标：< 数百 ms（取决于 MDS 与 KV 后端 IOPS）。

### A.7 一致性 / Crash 安全

事务隔离选 **SnapshotIsolation**（与现有 WriteSlice 一致）。

风险场景：

| 场景 | 行为 | 处理 |
|---|---|---|
| CopyFileRange 与 src 的 Compact/Unlink 并发 | A.4.2 步骤 2 的 Get 检测；SnapshotIsolation 兜底 | 失败重试 |
| CopyFileRange 与 dst 的 Unlink 并发 | dst inode 不存在 / version 变化触发 SI 冲突 | 失败重试 |
| MDS 在 CopyFileRange txn commit 前 crash | 事务原子性，无中间态 | 安全 |
| MDS 在 commit 后但客户端没收到响应 | 客户端可重试；幂等性需要 plan 阶段保证（建议 RPC 携带 client request id，CopyFileRange 在 txn 内做 idempotent check） | 待 plan 处理 |
| 程序 bug 导致悬挂 referrer（指向不存在的 ino） | 后台周期性"referrer 扫描修复"任务 | 新增 `gc.cc` 子任务，扫描 SliceReferrer，验证 (fs_id, ino) 仍存在且 chunk 仍含此 slice，否则删除该 referrer |

### A.8 配额

- dst 文件**逻辑大小**增长 → 计入 `used_bytes`（与 fallocate / 普通 write 一致）。
- 物理 block 是共享的：可选地新增 `physical_used_bytes` 统计字段（基于 SliceReferrer 反查 + slice.size 加权计算），但本期建议**不**实现，仅做"逻辑配额"准确。物理用量统计可以离线计算。

### A.9 落地到代码的改动清单（细化）

新增：
- `src/mds/common/codec.{h,cc}`：新增 `kMetaFsSliceReferrer` 编码、Encode/Decode 辅助、`GetSliceReferrerRange`、`ScanSliceReferrer(slice_id)` 帮助函数。
- `proto/dingofs/mds.proto`：`CopyFileRangeRequest/Response`、`MDSService.CopyFileRange`、`fs_info` 增加 `min_referrer_slice_id`。
- `src/mds/filesystem/store_operation.{h,cc}`：`CopyFileRangeOperation`。
- `src/mds/service/`：CopyFileRange RPC handler。
- `src/mds/background/gc.cc`：referrer 修复扫描子任务（次要）。

改造：
- `WriteSliceOperation::Run`（`store_operation.cc:1100+`）：新 slice 路径里追加 SliceReferrer Put。
- `CompactChunkOperation::Run`（`store_operation.cc:1983-2005`）：trash 判定接 SliceReferrer Scan。
- `DeleteFile` / unlink 路径里 chunk 清理处：同上。
- `gc.cc:CleanDelSlice`：可选兜底 Scan。

客户端：
- VFS 接口 + 实现 + meta_wrapper + mds_client。
- FUSE op 接线。

测试：
- 反向索引 codec round-trip。
- WriteSlice → SliceReferrer 正确登记。
- CopyFileRange 后 src compact/unlink 不会误删共享 block。
- 多客户端并发 reflink 同一 slice。
- 升级兼容：老 slice 走旧路径。
