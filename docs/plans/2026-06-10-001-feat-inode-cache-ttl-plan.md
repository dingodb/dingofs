---
title: "feat: 客户端 Inode 缓存属性 TTL"
type: feat
status: active
date: 2026-06-10
origin: docs/brainstorms/2026-06-10-inode-cache-ttl-requirements.md
---

# feat: 客户端 Inode 缓存属性 TTL

## Overview

为客户端 inode 属性缓存增加新鲜度 TTL（默认 5s，gflag 可动态调整）。`GetAttr` 缓存命中时校验"距上次 MDS 刷新时间"，超过 TTL 则同步从 MDS 重拉并经版本守卫合入缓存，解决多客户端下 inode 属性长期过期的问题。

## Problem Frame

多客户端挂载时，client-1 更新 inode 后 client-2 的缓存不感知。现有 `InodeCache::CleanExpired` 基于"最后访问时间"做空闲淘汰（每次 `Get` 刷新该时间），热点 inode 永不淘汰，导致活跃 inode 无限期返回过期属性。详见 origin 文档。

## Requirements Trace

（编号对应 origin 文档 R1–R7）

- R1. `Inode` 记录"最后一次从 MDS 数据刷新的时间"，与现有 `last_active_time_s_` 分离；本地 `Get` 不更新。
- R2. `GetAttr` 命中时校验该时间，超 TTL 视为未命中，同步重拉并经 `PutIf` 版本守卫合入。
- R3. 未过期命中路径无额外 RPC，性能无回退。
- R4. 新增 gflag `vfs_meta_inode_attr_ttl_s`，默认 5，`brpc::PassValidate` 支持动态调整。
- R5. TTL=0 表示每次 GetAttr 都回 MDS。
- R6. 重拉失败直接返回错误，不降级返回旧值（用户显式决策，含已知代价）。
- R7. 只改 `GetAttr` 路径。

## Scope Boundaries

- 不做 MDS 主动失效通知/lease 机制。
- 不改 `fuse_attr_cache_timeout_s` 默认值（内核侧 attr 缓存 10s 仍是端到端过期窗口的一部分）。
- 不改 `Lookup`、`IsInodeInTrash`、file_session、目录缓存、数据缓存。
- 不改现有空闲淘汰机制（`CleanExpired` / `vfs_meta_inode_cache_expired_s`）。
- 不做 single-flight 重拉合并（见 Key Technical Decisions）。

## Context & Research

### Relevant Code and Patterns

- `src/client/vfs/metasystem/mds/inode_cache.h` — `Inode` 已有 `std::atomic<uint64_t> last_active_time_s_` + `UpdateLastAccessTime()/GetlastActiveTime()` 范式，新字段照此实现（relaxed atomic，秒级时间戳 `utils::Timestamp()`）。
- `src/client/vfs/metasystem/mds/inode_cache.cc:33` — `Inode::PutIf`：版本守卫（`attr.version() <= version_` 则不更新属性）。注意它在版本检查**之前**调用 `UpdateLastAccessTime()`。
- `src/client/vfs/metasystem/mds/metasystem.cc:1380` — `MDSMetaSystem::GetAttr`：命中直接 `ToAttr`，未命中走 `mds_client_.GetAttr` + `PutInodeToCache`。
- `src/common/options/client.cc:87` — `DEFINE_uint64(vfs_meta_inode_cache_expired_s, 3600, ...)` + `DEFINE_validator(..., brpc::PassValidate)`：新 flag 照此模式；声明加在 `src/common/options/client.h:74` 附近。
- `test/unit/client/vfs/metasystem/` — 现有测试目录，CMake 用 `file(GLOB *.cc)` 自动收集，新增测试文件无需改 CMake。

### Institutional Learnings

- 无 `docs/solutions/` 目录，无可用沉淀。

### 架构约束（影响测试策略）

- `MDSMetaSystem` 以**值**持有 `MDSClient`（`src/client/vfs/metasystem/mds/metasystem.h:267`），现有 `MockMDSClient` 无法注入。因此 TTL 判定逻辑下沉到 `Inode`/`InodeCache` 层单测，`GetAttr` 路径改动保持薄（仅"过期→走既有冷路径"的分支）。

## Key Technical Decisions

- **刷新时间在 `PutIf` 中无条件更新（即使版本未变）**：重拉返回相同 version 也意味着 MDS 确认了数据新鲜，必须重置 TTL，否则过期 inode 将每次 GetAttr 都重拉。`PutIf` 的属性更新仍受版本守卫保护，两者解耦。
- **TTL 判定下沉为 `Inode` 的方法**（如 `IsAttrFresh(ttl_s)` 风格的只读判断）：可单测、调用方只加一行分支；不在 `InodeCache::Get` 内部做（R7 范围控制，避免影响其它"尽力读缓存"的调用方）。
- **不做 single-flight**：并发线程同时发现过期会产生少量重复 GetAttr RPC，`PutIf` 版本守卫保证正确性；5s TTL 下重复窗口极小，KISS 优先。如线上观测到放大问题再补（origin Deferred 问题在此关闭）。
- **过期路径复用既有冷路径代码**：过期等价于"未命中"，统一走 `mds_client_.GetAttr` + `PutInodeToCache`，不引入第二条重拉路径。
- **重拉失败返回错误**（origin R6，用户决策）：与现有冷路径失败行为一致。
- **重拉返回 not-found（`Status::NotExist`）时从缓存 `Delete` 该 inode**：inode 已在别处删除，保留缓存条目只会延长错误窗口（origin Deferred 问题在此关闭）。

## Open Questions

### Resolved During Planning

- 并发过期重拉是否需要 single-flight：不需要，接受少量重复 RPC（见上）。
- `CorrectAttr` 与重拉的顺序：保持现状——先取得新鲜 inode（缓存或重拉），`ToAttr` 后再 `CorrectAttr`，重拉不改变该顺序。
- ENOENT 处理：重拉返回 not-found（`MDSClient::GetAttr` 将 `pb::error::ENOT_FOUND` 映射为 `Status::NotExist`）时 `DeleteInodeFromCache` 并返回错误。
- `InodeCache::Load`（dump/restore）路径：恢复的 inode 刷新时间按构造时刻计，最多 5s 内可能略旧，可接受，不特殊处理。

### Deferred to Implementation

- 纯写入负载（`Write()` 不刷新 inode 缓存）的 TTL 重拉成本：实现后观测；如成为问题，后续在 flush 路径补缓存刷新（origin Deferred 问题，超出 R7 范围）。
- 是否为"TTL 过期重拉次数"加 bvar 指标：实现时视 `InodeCache` 现有 metrics（`total_count_`/`clean_count_`）模式顺手添加。

## Implementation Units

- [ ] **Unit 1: Inode 新增 MDS 刷新时间与新鲜度判定**

**Goal:** `Inode` 记录最后一次从 MDS 刷新的时间，并提供 TTL 新鲜度判定。

**Requirements:** R1, R5

**Dependencies:** None

**Files:**
- Modify: `src/client/vfs/metasystem/mds/inode_cache.h`
- Modify: `src/client/vfs/metasystem/mds/inode_cache.cc`
- Test: `test/unit/client/vfs/metasystem/test_inode_cache.cc`（新建）

**Approach:**
- 新增 `std::atomic<uint64_t> last_refresh_time_s_`，仿照 `last_active_time_s_` 的 relaxed 读写模式。
- 构造函数初始化为当前时间；`PutIf` 入口处无条件更新（在版本守卫之前，与 `UpdateLastAccessTime()` 并列）。
- 新增只读判定方法（入参 ttl 秒，返回是否仍新鲜）；ttl=0 时恒不新鲜。
- `InodeCache::Get`/本地读路径**不**触碰该字段。

**Patterns to follow:**
- `last_active_time_s_` + `UpdateLastAccessTime()/GetlastActiveTime()`（`inode_cache.h:222`、`inode_cache.cc:130`）。

**Test scenarios:**
- Happy path：新建 Inode 后立即判定，ttl=5 → 新鲜。
- Happy path：`PutIf` 收到更高 version → 属性更新且刷新时间更新。
- Edge case：`PutIf` 收到相同/更低 version → 属性不变（版本守卫生效）但刷新时间**仍更新**。
- Edge case：ttl=0 → 恒不新鲜。
- Edge case：模拟时间流逝（构造后手动设置旧刷新时间，或注入时间戳）超过 ttl → 不新鲜。
- Happy path：`InodeCache::Get` 不改变刷新时间（仅改变 last active time）。

**Verification:**
- 新增测试全部通过；现有 `CleanExpired` 行为（基于 last active time）不受影响。

- [ ] **Unit 2: 新增 TTL 配置 flag**

**Goal:** 提供 `vfs_meta_inode_attr_ttl_s` gflag，默认 5s，运行时可调。

**Requirements:** R4

**Dependencies:** None

**Files:**
- Modify: `src/common/options/client.cc`
- Modify: `src/common/options/client.h`

**Approach:**
- `DEFINE_uint64(vfs_meta_inode_attr_ttl_s, 5, "inode attr freshness ttl, refetch from mds after expired; 0 means always refetch")` + `DEFINE_validator(..., brpc::PassValidate)`，置于 `vfs_meta_inode_cache_expired_s`（client.cc:87）旁。

**Patterns to follow:**
- `src/common/options/client.cc:87-88` 的 flag + validator 配对。

**Test scenarios:**
- Test expectation: none —— 纯配置定义，行为由 Unit 1/3 测试覆盖。

**Verification:**
- 编译通过；flag 出现在二进制 `--help` 输出中。

- [ ] **Unit 3: GetAttr 路径接入 TTL 校验**

**Goal:** `GetAttr` 命中过期 inode 时走冷路径重拉；重拉失败返错；ENOENT 时清缓存。

**Requirements:** R2, R3, R6, R7

**Dependencies:** Unit 1, Unit 2

**Files:**
- Modify: `src/client/vfs/metasystem/mds/metasystem.cc`（`MDSMetaSystem::GetAttr`，~1380）

**Approach:**
- 命中后判定新鲜度：不新鲜则将 `inode` 置空（视为未命中），自然落入既有冷路径（`mds_client_.GetAttr` + `PutInodeToCache`）——单一重拉路径，R3 的未过期命中行为零改动。
- RPC 失败：返回 status（现有行为）；若 `status.IsNotExist()`（`MDSClient::GetAttr` 将 `pb::error::ENOT_FOUND` 映射为 `Status::NotExist`），先 `DeleteInodeFromCache(ino)` 再返回。
- `CorrectAttr` 顺序保持不变。
- 可选：仿 `clean_count_` 在 `InodeCache` 加过期重拉计数 bvar（实现时定夺）。

**Patterns to follow:**
- `GetAttr` 现有冷路径（metasystem.cc:1386-1395）；`DeleteInodeFromCache`（metasystem.h:213）。

**Test scenarios:**
（受 `MDSClient` 值持有无法 mock 的约束，GetAttr 分支逻辑以 Unit 1 的判定单测 + 以下集成验证覆盖）
- Integration（手动/双客户端环境）：client-1 `chmod`，client-2 在 TTL + 内核 attr 缓存窗口后 `stat` 看到新 mode。
- Integration（手动）：单客户端循环 `stat` 同一文件，抓 MDS RPC 计数确认 TTL 窗口内无额外 GetAttr RPC。
- Error path（代码评审确认）：重拉失败返回错误码与现有冷路径一致；`Status::NotExist` 路径调用了 `DeleteInodeFromCache`。

**Verification:**
- `cd build && make -j 12` 编译通过；`./build/bin/test/test_client`（vfs 单测二进制）通过。
- 双客户端手动场景符合 Success Criteria（≤ 5s + 10s 可见新属性）。

## System-Wide Impact

- **Interaction graph:** 仅 `MDSMetaSystem::GetAttr` 行为变化；`Open`/`Lookup`/`IsInodeInTrash`/file_session 等读缓存路径不变（R7）。FUSE 层 `getattr` 频率受内核 `attr_timeout`（10s）节流，TTL 重拉的 MDS 放大上限约为每客户端每活跃 inode 每 5s 一次。
- **Error propagation:** 新增错误面：过期重拉失败会使原本可命中缓存的 `stat()` 返错（用户已接受该代价，origin R6）。MDS 短暂不可用期间错误会跨客户端同时出现。
- **State lifecycle risks:** `PutIf` 无条件更新刷新时间 + 版本守卫更新属性，并发重拉无回退风险；ENOENT 清缓存避免死条目。
- **API surface parity:** 无对外 API 变化；新增一个 gflag。
- **Integration coverage:** 双客户端一致性场景无法单测，依赖手动/环境验证（见 Unit 3）。
- **Unchanged invariants:** 空闲淘汰（`CleanExpired`）语义不变；`last_active_time_s_` 用途不变；`Lookup` 仍每次走 MDS。

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| MDS GetAttr QPS 上升（每客户端每活跃 inode ≥ 每 5s 一次） | flag 动态可调（调大 TTL 即缓解）；内核 attr 缓存天然节流；可加 bvar 观测 |
| MDS 抖动放大为应用层 stat 错误 | 用户显式接受（origin R6 已记录代价）；运维侧可临时调大 TTL 规避 |
| 并发过期重拉的重复 RPC | PutIf 版本守卫保证正确性；量级小，暂不做 single-flight |
| 纯写入负载对自身文件的 TTL 重拉 | Deferred：实现后观测，必要时 flush 路径补刷新 |

## Sources & References

- **Origin document:** [docs/brainstorms/2026-06-10-inode-cache-ttl-requirements.md](../brainstorms/2026-06-10-inode-cache-ttl-requirements.md)
- Related code: `src/client/vfs/metasystem/mds/inode_cache.{h,cc}`、`src/client/vfs/metasystem/mds/metasystem.{h,cc}`、`src/common/options/client.{h,cc}`
