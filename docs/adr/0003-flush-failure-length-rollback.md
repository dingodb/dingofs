# 数据 flush 失败时条件式回退文件长度，而非撤销写入

数据 flush（`FileWriter::Flush`）失败时，客户端把 MDS 上的 `attr.length` 回退到 Flush 检查点
（本次 open 会话中最近一次 flush 成功时的长度，从未成功过则为 open 时的长度），即放弃这轮写。
我们不删除或物理撤销已通过 WriteSlice 提交的 slices，也不回滚 overwrite 内容——完整撤销需要
快照级机制，成本不成比例。MDS 在回退长度的同一事务中，为收缩区间追加零 slice，确保文件随后
重新扩展时不会暴露被截掉的数据；旧物理 slice 留给 compaction/GC 清理。

回退通过**扩展 FlushFile RPC**（rollback 标志 + checkpoint 长度）在 MDS 事务内条件执行：
仅当 `checkpoint < 当前 attr.length <= 本会话最后写长度` 时收缩。之所以必须条件式且在 MDS
侧原子执行，是因为 WriteSlice（`UpsertChunkOperation`）也会推大持久化长度，而并发写者可能
已把长度合法推进——无条件回退会砍掉别人已持久化的数据。我们只保证单写者正确性；多写者场景
保守放弃回退（残留 sparse 空洞，无数据损坏）。

其他刻意的取舍：

- **触发点仅限用户可见路径**（`VFSImpl::Flush` / `VFSImpl::Release`）。后台周期 flush 失败经
  sticky-broken 传导，在下一次用户可见 Flush/Release 时兜住。
- **尽力而为，无重试状态机**。每个用户可见失败点独立尝试一次回退；最终失败只记日志 + metric，
  用户仍收到原始 flush 错误。
- **回退不复位 writer**。sticky-broken 语义保持，用户必须重新 open 才能继续写（POSIX 应用对
  fsync 失败的标准应对，参见 fsync-gate）。
- **检查点由客户端维护**（file_session/ChunkSet），MDS 不新增 session 状态；客户端缓存过期的
  风险由条件式回退兜住。
- 复用 FlushFile 而非新增 RPC：复用现有 shrink 后处理链路（chunk 缓存失效、attr 回传）；
  proto3 默认值 0 作"未启用"哨兵，"截断到 0"由 rollback 标志区分。
- 收缩范围由 MDS 事务内读取的权威旧长度决定。零 slice 与 inode 长度原子提交，只修改收缩范围内
  已存在的 chunk（缺失 chunk 本身就是 hole）；事务无法容纳全部更新时整体失败，不做分批补偿。
- 收缩成功或 RPC 返回提交状态不确定的网络错误时，客户端保守失效 inode、chunk 和 read cache。

## 术语（写入与长度）

**MDS 持久化长度（Durable Length）**:
MDS 后端 inode `attr.length`。由 WriteSlice（`UpsertChunkOperation`，随 slice 提交推大）和
FlushFile 前移；SetAttr/truncate 可收缩。

**写长度备忘（Write Memo）**:
客户端 `chunk_set->last_write_length`，在 `MetaSystem::Write` 时即更新（数据可能仍只在客户端
内存）。GetAttr 用 `max(memo, attr.length)` 撑大内核可见长度。

**Flush 检查点（Flush Checkpoint）**:
某个 open 写会话中，最近一次数据 flush 完整成功时的文件长度；open 后从未成功过则为 open 时的
长度。由客户端在 file_session/ChunkSet 维护。长度回退的目标值。

**长度回退（Length Rollback）**:
数据 flush（`FileWriter::Flush`）失败时，把 MDS 持久化长度条件式收缩到 Flush 检查点，即放弃
这轮写。不物理撤销已提交的 slices，也不回滚 overwrite 内容；通过零 slice 保证被收缩的数据不会
在重新扩展后恢复。条件式：仅当
`checkpoint < 当前length <= 本会话最后写长度` 时收缩（单写者正确；多写者保守放弃）。仅在
用户可见失败点（Flush/Release）触发，尽力而为不重试。

**Sticky-broken**:
`FileWriter` 一旦 flush 失败即永久置坏，该 fh 上后续 Write/Flush 均返回该错误；长度回退后
依然保持，用户须重新 open 才能继续写。
