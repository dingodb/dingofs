# 小文件冷读场景：基于访问模式的目录级预热

**日期**：2026-05-12
**状态**：Requirements (待 plan)
**适用范围**：客户端 metasystem（`src/client/vfs/metasystem/mds/`）

---

## 1. 问题陈述

AI 训练等场景下，客户端会在同一目录下顺序/乱序地 `open + read` 大量小文件（典型如训练样本 jsonl/jpg）。当前痛点集中在**首次冷读**：

- 每个小文件都触发一次后端 S3 GET，单次延迟（几十 ms）远大于 payload 本身
- 客户端已有的 `TinyFileDataCache`、`InodeCache`、`PrefetchManager`、`WarmupManager` 均存在，但**没有自动触发机制**让 metasystem 主动告诉数据模块"接下来这一批小文件马上要读了，请提前拉"
- 现有 `WarmupManager` 主要靠手动 / 显式任务下发，对训练这种"我并不知道客户端要读哪些"的场景没有帮助

## 2. 目标用户与价值

- **用户**：使用 DingoFS 作为训练数据集存储的 AI 工程师
- **价值**：在不修改训练代码、不改动 S3 后端布局的前提下，把小文件冷读的端到端延迟从 "N × S3 GET" 收敛到 "1 × 触发延迟 + 并行预取"

## 3. 范围

### 3.1 In Scope
- 在客户端 metasystem 内部，基于 `Lookup` / `Open` 的访问模式检测，叠加 `ReadDir` 的结构性先验（目录子文件数 / 小文件占比）
- 当判定"同一目录正在被批量读取小文件"时，主动调用数据模块的预热接口
- 对单目录、单触发周期内的预热范围与并发做合理上限
- 通过现有 `WarmupManager` 的 `SubmitTask`（intime warmup 路径）下发，避免新增数据通路

### 3.2 Out of Scope
- 不改 S3 / 后端对象布局（不做小文件聚合打包）
- 不改 MDS 协议（不引入新的 readdir-plus 字段，复用现有 `AttrEntry` 中的 `maybe_tiny_file` / `length`）
- 不针对热缓存重复读优化（已有 `TinyFileDataCache` 覆盖）
- 不修改训练侧 DataLoader

## 4. 核心需求

### R1：目录级访问模式检测
metasystem 在 `Open`（必要时 `Lookup`）和 `ReadDir` 路径上维护**按父目录分片**的访问状态：

**Open / Lookup 信号（动态行为）：**
- 记录最近一段时间窗 `T` 内、对该 parent 下小文件的 open 次数
- 达到阈值 `N` 时认为进入"批量小文件读取模式"
- 状态需要带 TTL / 冷却时间，避免长生命周期目录持续重复触发

**ReadDir 信号（结构性先验）：**
- 在 `ReadDir` / `ReadDirPlus` 完成后，统计该目录下子文件总数与小文件占比
- 当小文件数 ≥ `M` 且小文件占比 ≥ `P%` 时，把该目录标记为"疑似批量小文件目录"
- 该标记影响后续 Open 检测的灵敏度：对已标记目录降低触发阈值 `N`（例如减半），甚至可以在标记时即触发首批小文件预热（受 `K` 上限约束）
- ReadDir 顺带把 attr 灌入 `InodeCache`，提升候选枚举（R3-A 路径）覆盖率
- 标记同样带 TTL，避免目录内容变化后仍按旧统计判断

### R2：小文件判定
- 复用现有 inode 上的 `length` 与 `maybe_tiny_file` 字段，不引入新字段
- "小文件"的尺寸阈值可配置，默认与 `vfs_tiny_file_max_size` 对齐或独立配置（待 plan 决策）

### R3：候选枚举
触发后，需要列出"该目录下还没被读、且为小文件"的候选 inode 列表。两种来源（待 plan 选其一或组合）：
- **A. InodeCache 内现存条目**：零额外 RPC，但覆盖率取决于此前是否 readdir 过
- **B. 触发一次 batched ReadDir（带 attr）拿全集**：覆盖率高，多一次 RPC，但相对总收益仍正向

### R4：候选过滤与上限
- 过滤已经在 `TinyFileDataCache` / 块缓存中命中的文件
- 单次触发的预热文件数上限 `K`（防止超大目录引发风暴）
- 同一目录在冷却时间 `C` 内只触发一次

### R5：下发预热
- 通过现有 `WarmupManager::SubmitTask` 提交 `WarmupTaskContext`（intime 类型）
- 不阻塞当前 `Open` 调用路径（异步提交）
- 复用 `PrefetchManager` 的 inflight 去重，避免重复拉取

### R6：可观测性
- 新增/复用 bvar 指标：触发次数、候选数、实际预热数、命中率（后续读命中预热结果的比例）、被丢弃的候选数
- 关键路径加 access_log，便于问题定位

### R7：开关与回退
- 整个特性可通过 gflag 一键关闭，默认开关由 plan 阶段决定
- 阈值 `N`、窗口 `T`、上限 `K`、冷却 `C`、小文件阈值均为 gflag

## 4.A 触发规则汇总（核心逻辑）

### 4.A.1 总体设计

预热判定分两个阶段，**ReadDir 是 Open 触发的前置条件**：

```
        阶段一：认知                    阶段二：执行
   ┌─────────────────────┐       ┌─────────────────────┐
   │ ReadDir 完成        │       │ Open(ino)           │
   │  ↓                  │       │  ↓                  │
   │ 统计 + 判定目录画像 │  ───→ │ 查父目录画像        │
   │  ↓                  │       │  ↓                  │
   │ 写入 DirProfile     │       │ 命中 + 满足条件     │
   │ 灌入 InodeCache     │       │  → 触发预热         │
   └─────────────────────┘       └─────────────────────┘
```

**关键约束**：父目录没有 `DirProfile` 记录时，Open 路径**完全不触发**预热（保守、零盲触发）。

### 4.A.2 参数定义

| 参数 | 含义 | 建议默认 |
|---|---|---|
| `S` | 小文件尺寸阈值（≤ S 视为小文件） | 与 `vfs_tiny_file_max_size` 对齐或独立 gflag |
| `M` | 目录被标记为"小文件目录"的小文件数下限 | 100 |
| `P` | 目录被标记为"小文件目录"的小文件占比下限 | 80% |
| `N` | Open 触发预热的窗口内小文件 open 次数阈值 | 5 |
| `T` | Open 计数滑动时间窗 | 10s |
| `K` | 单次触发预热文件数上限 | 256 |
| `C` | 同目录预热冷却时间 | 60s |
| `Tprofile` | DirProfile TTL（标记到期重新认知） | 300s |

### 4.A.3 阶段一：ReadDir → 建立 DirProfile

**触发**：每次 `ReadDir` / `ReadDirPlus` 完成。

**统计指标（仅基于本次返回页 + 已知 InodeCache 内的同 parent 项）：**
- `total_children` — 子项总数
- `small_file_count` — 满足 `type == FILE && length ≤ S` 的数量
- `small_file_ratio` — `small_file_count / total_children`
- `small_file_inos` — 该目录下的小文件 ino 集合（用于后续候选枚举的来源 A）

**判定与写入 `DirProfile`（按 parent ino sharded LRU 管理）：**

```
DirProfile {
  parent_ino
  is_small_file_dir       // small_file_count ≥ M && small_file_ratio ≥ P%
  total_children
  small_file_count
  small_file_inos         // 已知小文件 ino 集合（增量累加）
  warmed_inos             // 已下发预热的 ino 集合（避免重复）
  expire_ts               // = now + Tprofile
}
```

**增量行为**：
- 同一目录多次分页 readdir → 增量累加到同一 `DirProfile`，分页期间 `is_small_file_dir` 可滚动重算
- ReadDir 顺带把 attr 灌入 `InodeCache`
- **失效策略**：仅按 `Tprofile` TTL 失效。**不感知目录 mtime 变化**——预热是 best-effort，目录内容变化最多导致预热到不存在/已变的 ino（被 `WarmupManager` 自然忽略，浪费少量带宽），换来 Open 路径上零额外 inode/lookup 成本

### 4.A.4 阶段二：Open → 结合 DirProfile 决定是否预热

**触发**：每次 `Open(ino)` 成功后。

**步骤：**

1. **快速短路**：
   - 取 inode 的 `length` / `type`，若非 FILE 或 `length > S` → 返回（不计数、不触发）
   - 取 parent ino，查 `DirProfile`：
     - 不存在 → 返回（**没认知就不动作**）
     - `is_small_file_dir == false` → 返回
     - 在冷却中（`now < cooldown_until`） → 返回

2. **窗口计数**（per-DirProfile）：
   - 若窗口过期：重置 `open_count = 0`、`window_start = now`
   - `open_count++`，记录本次 ino 到 `recently_opened`

3. **阈值判定**：
   - `open_count ≥ N` → 进入「执行预热」

### 4.A.5 执行预热流程

1. **去重门禁**：置 `cooldown_until = now + C`、清零 `open_count`
2. **候选枚举**（基于 `DirProfile.small_file_inos`）：
   - 候选 = `small_file_inos − warmed_inos − recently_opened`
3. **过滤**：
   - 排除已经在 `TinyFileDataCache` 中且 complete 的 ino
   - 排除已在 `PrefetchManager.inflight_keys_` 中的 block
   - 截断到上限 `K`
4. **下发**：构造 `WarmupTaskContext`（intime 类型），异步 `WarmupManager::SubmitTask`，**不阻塞当前 Open**
5. **登记**：被下发的 ino 加入 `warmed_inos`，避免下一轮重复
6. **指标**：触发次数 +1、候选数、实际下发数、被过滤数

### 4.A.6 抑制与回退

| 场景 | 行为 |
|---|---|
| Parent 没有 DirProfile | Open 不触发预热（前置条件不满足） |
| DirProfile 标记非小文件目录 | Open 不触发预热 |
| 同目录冷却期内 | 跳过 |
| 候选枚举为空 | 仅刷新冷却，不下发 |
| `WarmupManager` 失败 | 记录指标，按错误处理（不重试） |
| 目录被删除 / mtime 变 | 不主动感知；DirProfile 按 TTL 自然到期。中间下发的预热目标若已不存在，由 `WarmupManager` 路径自然忽略 |
| 整个特性 gflag 关闭 | ReadDir / Open hook 短路返回，零开销 |

### 4.A.7 决策表（一图看懂）

| Parent DirProfile | 是否小文件目录 | 是否冷却中 | Open 事件 | 行为 |
|---|---|---|---|---|
| 无 | — | — | 任意 | 不触发 |
| 有 | 否 | — | 任意 | 不触发 |
| 有 | 是 | 是 | 任意 | 不触发（冷却） |
| 有 | 是 | 否 | Open 大文件 / 非文件 | 仅短路返回 |
| 有 | 是 | 否 | Open 小文件，count < N | 计数 +1 |
| 有 | 是 | 否 | Open 小文件，count == N | **触发预热** |

## 5. 成功标准

- 训练加载场景下，目录预热命中率 ≥ 70%（命中即"被预热的文件后续确实被读且命中缓存"）
- 小文件冷读端到端延迟（p50 / p99）相比当前版本明显下降（具体目标在 plan 里定基线后给出）
- 在非小文件场景（大文件读、随机大目录浏览）下，CPU / 内存 / RPC 量无可观测回归
- 关闭开关后，行为与当前完全一致

## 6. 关键决策与待定事项

| 项 | 现状 | 决策 |
|---|---|---|
| 触发时机 | 多种可选 | **已定**：`Open` 模式检测 + `ReadDir` 结构性先验；`Lookup` 是否参与待 plan |
| 后端布局 | 不动 | **已定** |
| 候选来源（R3） | A vs B | 待 plan：建议 A 优先 + B 兜底（首次触发时若候选不足则发一次 ReadDir）。ReadDir 信号路径天然填充 InodeCache，使 A 命中率显著提升 |
| 小文件阈值 | 复用 vs 独立 | 待 plan |
| ReadDir 触发预热 | 标记即触发 vs 仅降阈值 | 待 plan：建议默认仅降阈值，gflag 可开启"标记即触发首批" |
| 默认开关 | — | 待 plan |
| 内存上限 | per-dir 状态需要上限 | 待 plan：建议 sharded LRU |

## 7. 风险与权衡

- **过度预热**：训练只读其中一部分时浪费带宽 → 通过 `K` 上限 + 命中率监控控制
- **目录状态膨胀**：极多目录访问时 per-dir 状态占用内存 → sharded LRU + TTL 清理
- **与 readdir 预热的关系**：本期不做，但要保持设计可扩展（`SmallFileWarmupDetector` 应能容纳新触发源）
- **多客户端重复预热**：每个客户端独立判定，可能多客户端同时拉同一批文件 → 依赖远端 cache group 去重，本期不解决

## 8. 涉及代码区域（仅供 plan 参考）

- `src/client/vfs/metasystem/mds/metasystem.{h,cc}` — `Open` / `Lookup` 钩子
- `src/client/vfs/metasystem/mds/inode_cache.{h,cc}` — 候选枚举来源 A
- `src/client/vfs/components/warmup_manager.{h,cc}` — 复用 `SubmitTask`（intime）
- `src/client/vfs/components/prefetch_manager.{h,cc}` — inflight 去重
- 新增：目录访问模式检测器（位置由 plan 决定，建议 `src/client/vfs/metasystem/mds/` 下新模块）

---

## 9. 下一步

进入 `/ce-plan`，重点产出：
1. 模式检测器的数据结构与并发模型
2. 候选枚举策略（A / B / A+B）的最终选择与实现细节
3. 与 `WarmupManager` 集成的具体调用点
4. gflag 列表与默认值
5. 单元测试与集成测试清单
