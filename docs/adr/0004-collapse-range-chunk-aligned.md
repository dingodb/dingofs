# FALLOC_FL_COLLAPSE_RANGE 仅支持 chunk 对齐，单事务元数据平移实现

Fallocate 支持 `FALLOC_FL_COLLAPSE_RANGE`：删除 `[offset, offset+len)` 并把后续数据
逻辑左移。我们要求 `offset` 和 `len` 都按 `chunk_size` 对齐，否则返回 EINVAL——内核
允许文件系统自定对齐粒度（ext4/XFS 要求 block 对齐），FUSE 层 EINVAL 是合法答复。

对齐到 chunk 使 collapse 退化为**纯元数据操作**：删除范围内的 chunk 行，把后续每个
chunk 的 index 减 `len/chunk_size` 重写到新 key，slice 内容一字节不动。字节级/block
级对齐则需要跨 chunk 重排 slice pos、拆分跨界 slice，并重写全部后续 slice 映射，
复杂一个数量级；当前没有需要非对齐 collapse 的真实用例。

## 关键取舍

- **单事务提交**。删旧 key + 写新 key + 更新 `attr.length` 在一个事务内原子完成，
  与现有 `Operation` 批处理框架（单 txn 模型）一致；并发 WriteSlice 靠 txn 冲突保证
  安全。天花板：超大文件（后续 chunk 行过多）可能超出 DingoStore/TiKV 事务大小限制
  而整体失败，不做分段补偿——collapse 的典型用例（截掉文件头部）不涉及 PB 级文件。

- **禁止 flag 组合，边界按内核语义**。`mode` 必须全等于 `FALLOC_FL_COLLAPSE_RANGE`；
  `len == 0` 或 `offset + len >= file_length` 返回 EINVAL（删整个尾部应使用
  truncate）。校验在 VFS 层（省 RPC）和 MDS `FileSystem::Fallocate` 入口双侧执行，
  MDS 不信任客户端。推论：文件尾部不足一个 chunk 的零头永远无法被 collapse 波及，
  这是对齐约束的自然结果，无需特判。

- **平移后 chunk version = max(源位置 version, 目标位置原 version) + 1**。客户端
  chunk 缓存靠 version 比对（ReadSlice 的 ChunkDescriptor）刷新；平移使 index N+k
  的内容出现在 index N，若 version 不压过目标位置的历史值，持有旧缓存的客户端会
  读到已删除的数据。MDS 侧 `chunk_cache_` 对该 ino 整体失效（collapse 罕见，粗暴
  清光最省事）。跨客户端的旧数据暴露窗口与现有 truncate/PUNCH_HOLE 一致，不引入
  新的一致性模型。

- **被删 chunk 的 slice 回收复用 compact 的 GC 路径**。删除范围内每个 chunk 在同一
  事务写一条 DelSlice 记录（`TrashSliceList`）；后台 GC（`ScanDelSlice` →
  `CleanDelSliceTask`）负责 SliceRef 减引用、refcount 归零后删 S3 block。平移的
  chunk slice id 不变，SliceRef 不动。零新回收逻辑。

- **记账沿用 delta_bytes**。`delta_bytes = new_length - old_length`（负值），走现有
  quota/dir-stat 异步记账路径；物理空间回收滞后于 GC，与 truncate-down/删除文件的
  现状一致。

## 术语

**Collapse（区间折叠）**:
删除文件的一段字节区间并把其后的数据逻辑左移，文件长度减小 `len`。区别于
PUNCH_HOLE（遮盖为零、长度不变）和 truncate（只砍尾部）。

**Chunk 平移（Chunk Shift）**:
collapse 的实现手段：后续 chunk 行整体改写到 `index - len/chunk_size` 的新 key，
slice 内容不动。仅在 chunk 对齐前提下成立。
