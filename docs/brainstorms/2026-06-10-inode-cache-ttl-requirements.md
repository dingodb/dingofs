---
date: 2026-06-10
topic: inode-cache-ttl
---

# 客户端 Inode 缓存 TTL（多客户端属性一致性）

## Problem Frame

多客户端挂载同一文件系统时，client-1 更新 inode（chmod、truncate、setxattr 等）后，client-2 的本地 inode 缓存不会感知该变更。现有的 `InodeCache::CleanExpired`（`src/client/vfs/metasystem/mds/inode_cache.cc`）是基于 `last_active_time_s_` 的**空闲淘汰**——该时间戳在每次 `Get` 时刷新，因此被持续访问的热点 inode 永远不会被清理，client-2 上的活跃 inode 可无限期地返回过期属性。

`MDSMetaSystem::GetAttr`（`src/client/vfs/metasystem/mds/metasystem.cc:1380`）在缓存命中时直接返回，从不校验数据新鲜度。`Lookup` 每次都走 MDS RPC 并通过版本守卫的 `PutIf` 刷新缓存，因此不受此问题影响。

修复方式：为缓存的 inode 属性引入**新鲜度 TTL**——基于"最后一次从 MDS 刷新的时间"（而非"最后访问时间"），过期后强制从 MDS 重新拉取。

## Requirements

**TTL 语义**

- R1. `Inode` 记录"最后一次从 MDS 数据刷新的时间"（构造时和 `PutIf` 收到 MDS 数据时更新；本地 `Get` 访问**不**更新），与现有 `last_active_time_s_`（空闲淘汰用）分离。
- R2. `GetAttr` 缓存命中时校验该刷新时间：距今超过 TTL 则视为缓存未命中，同步调用 `mds_client_.GetAttr` 重新拉取，并通过现有 `PutIf` 版本守卫合入缓存（版本号不回退）。
- R3. 未过期的命中路径行为不变（无额外 RPC，性能无回退）。

**配置**

- R4. 新增 gflag（如 `vfs_meta_inode_attr_ttl_s`），默认 **5 秒**，定义于 `src/common/options/client.cc`，使用 `brpc::PassValidate` 支持运行时动态调整（与现有 flag 一致）。
- R5. TTL = 0 表示属性永不视为新鲜（每次 GetAttr 都回 MDS）；如需保留"永不过期"的旧行为，可将 TTL 调大。

**失败行为**

- R6. 过期后重拉 MDS 失败时，**直接向调用方返回错误**（一致性优先），不降级返回缓存旧值。该行为与现有冷路径（缓存未命中且 RPC 失败）一致。已知代价（评审确认后用户维持此决策）：MDS 短暂不可用期间，所有客户端上 TTL 过期的活跃 inode 的 `stat()` 会同时报错。

**生效范围**

- R7. 本次只改 `GetAttr` 路径。`Lookup`（本就每次走 MDS）、`IsInodeInTrash`、file_session 等其它读缓存路径保持现状。

## Success Criteria

- 两客户端场景：client-1 修改 inode 属性后，client-2 在 TTL + 内核 attr 缓存窗口内通过 `stat` 看到新属性（默认配置下 ≤ 5s + 10s）。
- 单客户端高频 `stat` 同一文件：TTL 窗口内不产生额外 MDS RPC。
- 已有单元测试不回归；新增 TTL 过期/未过期/重拉失败的单元测试。

## Scope Boundaries

- 不做 MDS 主动失效通知（invalidation/lease 机制）——那是更大的架构改动。
- 不改 FUSE 内核层 `fuse_attr_cache_timeout_s`（默认 10s）的默认值；端到端过期窗口 = 内核 TTL + 客户端 TTL，部署时可按需调小内核侧。
- 不改 `Lookup`、目录缓存、数据（chunk）缓存的一致性。
- 不改现有空闲淘汰机制（`CleanExpired` / `vfs_meta_inode_cache_expired_s`）。

## Key Decisions

- **惰性校验而非后台主动刷新**：只在读路径按需重拉，实现简单、无额外后台 MDS 压力；后台刷新会对全缓存（可能数十万 inode）产生周期性 RPC 风暴。
- **TTL 默认 5s**：用户选择更强一致性；MDS 压力可通过动态调 flag 缓解。
- **重拉失败返回错误**：用户选择一致性优先；避免应用在 MDS 抖动期间静默读到旧属性。
- **基于"MDS 刷新时间"而非"访问时间"**：访问时间会被本地读刷新，无法表达数据新鲜度——这正是现有机制无法解决该问题的原因。

## Dependencies / Assumptions

- `AttrEntry.version` 在 MDS 侧单调递增，`PutIf` 的版本守卫足以防止并发重拉导致的属性回退（现有机制，已验证存在于 `inode_cache.cc:42`）。
- 只有返回新鲜 `AttrEntry` 的 RPC 路径（SetAttr、Create/MkNod、xattr 操作、Lookup 等）会经 `PutInodeToCache` 刷新缓存并更新刷新时间；**`Write()` 不会**（metasystem.cc 的写快路径只更新 chunk/modify-time memo）。因此长时间纯写入的客户端对自己正在写的文件也会按 TTL 周期性重拉 GetAttr，直到 flush/setattr 刷新缓存。

## Outstanding Questions

### Deferred to Planning

- [Affects R2][Technical] 并发过期重拉的抑制：多线程同时发现同一 inode 过期时是否需要 single-flight 合并 RPC，还是接受少量重复 GetAttr（PutIf 已保证正确性）。
- [Affects R2][Technical] `CorrectAttr` 路径与重拉逻辑的交互顺序确认。
- [Affects R6][Technical] 重拉返回 `ENOENT`（inode 已在别处删除）时应同时从缓存 `Delete` 该 inode。
- [Affects R2][Technical] 纯写入工作负载：`Write()` 不刷新 inode 缓存，活跃写入者对自己的文件也会付出 TTL 重拉成本——是否可接受，或需在 flush 路径补一次缓存刷新。

## Next Steps

-> /ce:plan 进行结构化实现规划
