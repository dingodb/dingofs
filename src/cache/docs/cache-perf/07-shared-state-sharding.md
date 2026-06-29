# 缓存服务端共享可变状态的 per-core / 分片化总体策略

## 概述

本文是缓存服务端（`src/cache/`）性能优化系列的**横切总纲**，目标是给出一套统一的方法论，把当前 cache server 中以单把 `bthread::Mutex` / `bthread::RWLock` 守护的**共享可变状态**，系统性地改造为 **per-core / 分片化（lock-striping）/ lock-free / 保持不动** 四类形态，从而消除跨核 cache-line 争用与序列化瓶颈。

本文是横切策略，**不重复展开**两个已有专题：

- 磁盘 LRU 锁分片 → 见 **01 文档**（`DiskCacheManager::mutex_` / `ShardedLRUCache` 的分片改造）。
- 上传队列分片 → 见 **04 文档**（`BlockCacheUploader::PendingQueue` 的分片改造）。

本文聚焦其余共享结构：`InflightTracker`、下载 singleflight（`TaskTracker`）、连接池（`StorageClientPool` 及远端连接池）、全局 slab 池（`SlabBufferPool` + `MemoryPool`）、统计 bvar、以及若干 infiniband / remote 子系统的共享 map，并把它们汇成一张**改造矩阵**。

> 范式参照：`src/cache/local/mem_cache.{h,cc}` 已经落地了一个干净的 32-shard、`alignas(64)` 分片模板，本文所有"按 key 分片"的改造都对齐它的写法。

---

## 背景与动机

### 当前并发模型：bthread/brpc 的 M:N 线程池，不是 share-nothing reactor

必须先澄清一个关键事实，否则后续讨论会失真：

**cache server 当前并非 Seastar 式 share-nothing per-core reactor，而是 brpc 的 bthread M:N 协程模型。**

证据：

- `src/cache/node/cache_server.cc:54` 只 `make_unique<CacheNode>()` 一个节点；`src/cache/node/node.cc:87` 只持有一个 `block_cache_`。整个进程内 `CacheNode`、`LocalBlockCache`、`TaskTracker`、`StorageClientPool` 都是进程级单例，被所有请求 bthread 共享。
- `src/cache/node/cache_server.cc` 的主循环是 `brpc_server_->RunUntilAskedToQuit()`，每个 RPC 被调度到一个 bthread 上，bthread 被 M:N 复用到一组 worker pthread。
- 全子系统的锁都是 `bthread::Mutex` / `bthread::RWLock` / `bthread::ConditionVariable`（bthread 感知，争用时让出 bthread 而非阻塞 pthread）。
- 现有的"分片"结构（`mem_cache` 32 shard、infiniband `Waiters` 512 shard、`ShardedLRUCache` 64 shard、`MemoryPool` 32 shard）都是 **lock-striping**：shard 并未绑核，任意核上的任意 bthread 都可能命中任意 shard。

因此本文标题中的 "per-core" 在 DingoFS cache server 语境下有两层含义，需要分别对待：

1. **近期可落地（本文主体）**：在 bthread 模型下，把中心锁拆成 N 路 **lock-striping** 分片 / per-worker 实例 / lock-free，N 取 `worker pthread 数`（或其 2 的幂上界），并用 `alignas(64)` 消除伪共享。这是"逼近 per-core"而非真正 per-core。
2. **远期可选（仅在 §统一方法论 中点到）**：若未来引入 brpc 的 `bthread tag` / worker 亲和，或迁移到真正的 per-core executor，再把"按 key hash 选 shard"升级为"按当前 worker 选本地 shard"，实现真 share-nothing。本文的改造矩阵刻意让两步**前向兼容**——第一步选好的分片粒度，第二步只需把"选 shard 的函数"从 `hash(key) & mask` 换成 `current_worker_id()`，数据结构不变。

### 为什么这些结构是瓶颈

在高并发（数千 inflight）下，下列单锁结构会让所有 worker 在同一把锁 / 同一条 cache-line 上排队：

- `InflightTracker::mutex_`：cache(1024) / prefetch(1024) / upload(32) / 盘 inflight(iodepth=128) 四条流水线，每条都是**一把**全局 `bthread::Mutex`，每个 block 的进/出都要 `Add`/`Remove`。
- `TaskTracker::mutex_`：每次从存储下载都要进这把锁查/插 singleflight map。
- `StorageClientPool::rwlock_`：每个 Put/Range 路径都要读锁查 `clients_`。
- `MemoryPool`：已是两层 lock-free，但仍是**中心分配器**——layer-1 的 `caches_[128]` 是按"线程 slot"近似 per-thread，layer-2 的 32 个 Treiber 栈仍跨核共享。
- 几个 infiniband / remote 的 `map` + mutex。

代码里作者已自标两处热点：`disk_cache_manager.cc:164` 的 `// FIXME: lock contention`（归 01 文档），`buffer_pool.h:39` 的 `// TODO: shared bufferpool if lock contention is high`。

---

## 共享状态全清单与改造矩阵

下表是本系列的**单一事实来源**。"改造方案"列取值：**per-worker 实例** / **key 分片** / **lock-free** / **保持** / **引用专题文档**。

| # | 结构 | 位置 | 当前锁/原子 | 守护内容 | 键 | 改造方案 | 说明 |
|---|---|---|---|---|---|---|---|
| 1 | `InflightTracker`（cache/prefetch） | `iutil/inflight_tracker.h:37-71`；构造 `local/local_block_cache.cc:96-97`（各 1024）、`tier/tier_block_cache.cc:71-73` | `bthread::Mutex`+condvar | inflight 计数 + `busy_` 去重集合 | filename | **key 分片**（N 路 `Shard`，全局限额按 shard 等分） | 见 §统一方法论"分片限流器" |
| 2 | `InflightTracker`（upload） | 构造 `local/block_cache_uploader.cc:149-150`，`FLAGS_upload_stage_max_inflights=32` | 同上 | 同上 | stage key | **key 分片** + **配合 04 文档** | 限额仅 32，分片后每片限额需向上取整，见 §风险 |
| 3 | `InflightTracker`（盘 AIO） | 内嵌 `local/local_filesystem.cc:129`，`FLAGS_iodepth=128`；guard 在 `:113-123` | 同上 | 同上 | fd 字符串 | **per-worker 实例** 或保持 | 一个盘一个 LFS，键是 fd；见 §说明 1 |
| 4 | `TaskTracker`（下载 singleflight） | `common/task_tracker.h:85-112`；构造 `node/node.cc:83` | `bthread::Mutex` + 每 task 自带 mutex/cond | `tasks_` 去重 map | filename | **key 分片** | singleflight 语义在分片下保持，见 §统一方法论 |
| 5 | `StorageClientPool` | `common/storage_client_pool.{h,cc}`，`storage_client_pool.h:71-74` | `bthread::RWLock` | `accessors_` + `clients_`（per-fs_id） | fs_id | **保持** + 可选 lock-free 读 | 读多写极少（懒建一次），RWLock 读路径已轻；见 §说明 2 |
| 6 | 全局 `SlabBufferPool` ×2 | `common/slab_buffer.{h,cc}:111-136` | 无锁（委托 MemoryPool） | read/write slab buffer | 全局单例 | **保持**（薄壳） | 真正争用在底层 MemoryPool |
| 7 | `MemoryPool`（slab 底座） | `common/writemempool/memory_pool.{h,cc}:75-114` | 128× `atomic_flag` cache + 32× atomic Treiber 栈 | 4MiB buffer 空闲链 | 线程 slot 哈希 | **lock-free（已是）** → 可选升级 **per-core arena** | 取舍见 §统一方法论"slab per-core arena" |
| 8 | 统计 bvar | `common/vars.h`、`mem_cache.h:68-73`、`disk_cache_manager.h:62-67`、`node.h:82-83` 等 | bvar 内部 per-thread 聚合 | 计数/延迟 | — | **保持** | bvar 本就 per-thread 聚合，读时合并，无需动 |
| 9 | `DiskCacheManager::mutex_` | `local/disk_cache_manager.h:121`（自标 FIXME） | `bthread::Mutex` | LRU + `staging_blocks_` | per-disk | **引用 01 文档** | 本文不展开 |
| 10 | `ShardedLRUCache` | `iutil/cache.cc:415-421` | 64× `bthread::Mutex` + `id_mutex_` | hash table | hash>>26 | **引用 01 文档** | 已 64 分片，01 文档评估是否够 |
| 11 | `BlockCacheUploader::PendingQueue` | `local/block_cache_uploader.cc:138-140` | `bthread::Mutex` | 优先级队列 | BlockFrom | **引用 04 文档** | 本文不展开 |
| 12 | `MemCache` shards | `local/mem_cache.h:123-128` | 32× `bthread::Mutex` | LRU+index/shard | hash(key)&31 | **保持（范式）** | 即改造模板本身 |
| 13 | infiniband `Waiters` | `infiniband/waiter.h:84-98` | 512× `bthread::Mutex` | corr_id→Waiter | hash&511 | **保持** | 已重度分片，per-session |
| 14 | `RemoteCacheCluster` | `remote/remote_cache_cluster.h:192-193` | `bthread::RWLock` | `group_`（COW 整体替换） | 全局 | **保持（已 COW）** | 读取仅取一次指针即 lock-free |
| 15 | `RemoteNode` 连接池 | `remote/remote_node.h:85` | `atomic<int>` 轮转 index | 16 条连接 vector | per-node | **保持（已 lock-free）** | 固定大小，原子轮转 |
| 16 | `TCPConnection`/`RDMAConnection` | `remote/remote_node_connection.h:97,121` | RWLock / Mutex | brpc channel / rdma client | per-conn | **保持** | 连接级，争用低 |
| 17 | infiniband `EventDispatcher` | `infiniband/event.h:91` | `bthread::Mutex` | fd→handler map | per-dispatcher | **保持** | 仅注册/注销期写，epoll 线程读 |
| 18 | legacy `BufferPool` | `iutil/buffer_pool.h:39-93`（自标 TODO） | `bthread::Mutex`+condvar | `free_indices_` 队列 | per-pool | **淘汰/迁移到 MemoryPool** | 若仍在用，迁到 #7 的分片底座 |

> 矩阵口径：1–4、18 是本文重点改造对象；5、6、14–17 论证为"保持"；7 给出可选 per-core arena 的取舍；9–11 引用专题文档；8、12、13 保持。

---

## 统一分片方法论

下面给出可复用于上表所有"key 分片 / per-worker"改造的统一模板，并逐个落到具体结构。

### M1. 分片基本模板（对齐 `mem_cache`）

固定写法，照搬 `mem_cache.h:80-148`：

```cpp
class XxxSharded {
 public:
  static constexpr size_t kShardCount = 64;  // 2 的幂，覆盖 worker 数
  static_assert((kShardCount & (kShardCount - 1)) == 0, "power of 2");

  static size_t ShardIndex(const std::string& key) {
    return std::hash<std::string>{}(key) & (kShardCount - 1);
  }

 private:
  struct alignas(64) Shard {           // 必须 alignas(64) 消除伪共享
    mutable bthread::Mutex mutex;
    /* 该结构的局部状态 */
  };
  Shard& GetShard(const std::string& key) { return shards_[ShardIndex(key)]; }
  std::array<Shard, kShardCount> shards_;
};
```

要点：

- **shard 数取 2 的幂**，掩码取模；选 `>=` worker pthread 数的最小 2 的幂（典型 32 或 64）。
- **`alignas(64)`** 是硬性要求，否则相邻 shard 的 mutex 落同一条 cache-line，分片白做。
- 每个 shard **自带局部计数器**，全局量按需在读路径上聚合（如 `Dump` 遍历求和，见 `mem_cache.cc:168-181`），写路径不碰全局。
- **前向兼容真 per-core**：把 `GetShard(key)` 抽成一个可替换点。第一步用 `hash(key)`；将来若有 worker 亲和，只改成 `shards_[current_worker_id() & (kShardCount-1)]`，shard 内部结构完全不动。

### M2. 分片限流器（#1/#2/#3 `InflightTracker`）

`InflightTracker` 同时承担两件事——**全局 inflight 限额** 与 **同 key 去重**。分片时两者都要保语义：

```cpp
class ShardedInflightTracker {
  static constexpr size_t kShardCount = 64;
  struct alignas(64) Shard {
    bthread::Mutex mutex;
    bthread::ConditionVariable cond;
    uint64_t inflights{0};
    uint64_t max_inflights;          // = ceil(total_max / kShardCount)
    std::unordered_set<std::string> busy;
  };
  std::array<Shard, kShardCount> shards_;
 public:
  Status Add(const std::string& key) {
    auto& s = shards_[ShardIndex(key)];
    std::unique_lock<bthread::Mutex> lk(s.mutex);
    if (s.busy.count(key)) return Status::Exist("task already running");
    while (s.inflights + 1 > s.max_inflights) s.cond.wait(lk);
    s.busy.emplace(key); s.inflights += 1; return Status::OK();
  }
  void Remove(const std::string& key) { /* 对称，notify_all */ }
};
```

去重语义**天然安全**：同一个 key 永远落同一个 shard，`busy` 检查在 shard 内仍是全局唯一判定。

限额语义**有近似**：全局 `max_inflights` 被等分到各 shard（`per_shard = ceil(total/kShardCount)`）。

- **cache/prefetch（#1，1024）**：1024/64≈16，分片粒度足够，分布均匀，几乎无副作用。
- **upload（#2，32）**：32 < 64，等分后每片只有 0~1，限流形同虚设。**对策**：upload tracker 的 shard 数取 `min(kShardCount, total_max)`（即 32 取 16 路、每片 2），或干脆把限额逻辑交给 04 文档的分片上传队列、tracker 仅保留去重职责。详见 §风险与对策。
- **盘 AIO（#3，128，键=fd）**：这是**单盘**的 LocalFileSystem 内嵌实例，限的是单盘并发 AIO 深度，键是 fd 不是 block。它本身已经是"per-disk 实例"，不属于全局争用热点；可**保持**，或在确有争用时按 `fd & mask` 做 per-worker 分片（fd 整数取模比字符串 hash 更省）。

### M3. 分片 singleflight（#4 `TaskTracker`，share-nothing 下如何按核做）

singleflight 的不变量是：**同一个 key 在任一时刻只有一个 leader 真正下载，其余 follower 等待复用结果。** 这个不变量只要求"同 key 的所有请求看到同一张 map"，**不要求全局只有一张 map**。

因此分片 singleflight 的正确做法是 **按 key 分片，而非按核分片**：

```cpp
class ShardedTaskTracker {
  struct alignas(64) Shard {
    bthread::Mutex mutex;
    std::unordered_map<std::string, DownloadTaskSPtr> tasks;
  };
  std::array<Shard, kShardCount> shards_;
 public:
  bool GetOrCreateTask(const BlockHandle& h, size_t len, DownloadTaskSPtr& t) {
    auto& s = shards_[ShardIndex(h.Filename())];   // 同 key 必同 shard
    std::lock_guard<bthread::Mutex> lk(s.mutex);
    auto it = s.tasks.find(h.Filename());
    if (it != s.tasks.end()) { t = it->second; return false; }
    t = std::make_shared<DownloadTask>(h, len); s.tasks[h.Filename()] = t; return true;
  }
  void RemoveTask(const BlockHandle& h) { /* 同 shard erase */ }
};
```

`DownloadTask` 自身的 per-task `mutex_/cond_/finish_`（`task_tracker.h:79-81`）**完全不动**——它本来就是每任务一把锁，leader `Run()` → `notify_all`，follower `Wait()`，与分片正交。

**为什么不能"按核分片 singleflight"**：若每个 worker 持一张本地 map，同一个 block 的两个请求落在不同 worker 时会各自成为 leader，重复下载、击穿去重，违反不变量。所以即便将来迁到真 per-core，singleflight 的 map 也**必须按 key 路由**（key→shard→可能跨核访问该 shard），这是 singleflight 与无状态限流器的根本区别，也是本文要点之一。

### M4. slab 改 per-core arena 的取舍（#7 `MemoryPool`）

`MemoryPool`（`memory_pool.h:38-114`）**已经是两层 lock-free**，本身就是 slab 的合理终态，不应盲目再加一层：

- **Layer-1** `caches_[128]`：每个 `alignas(64)` cache 用 `atomic_flag` try-lock，按 `ThreadSlot() % 128` 近似 per-thread，稳态 ~99% 命中、~10ns/op、无跨核 coherence 流量。
- **Layer-2** `shards_[32]`：32 个 `alignas(64)` Treiber 栈，cache 批量 refill/flush（每次 CAS 搬 `kRefillBatch=4` 个），把竞争原子摊薄。

**是否进一步改 per-core arena（每核一段独占的 buffer 子区 + 本地空闲链）？给出取舍：**

收益（仅在以下成立时才考虑）：

- 真正切到 per-core executor 且 worker 绑核后，layer-1 的"近似 per-thread"可升级为"严格 per-core"，连 `atomic_flag` 的 try-lock 都能省掉（单核内无并发，纯本地链表 push/pop）。
- 彻底消除 layer-2 的跨核 CAS。

代价 / 风险：

- **碎片与利用率**：buffer 总量固定（`4MiB × FLAGS_iodepth`，`slab_buffer.cc:126-128`），切成 per-core 后每核分到的更少；某核突发大量请求会本核耗尽而其他核空闲，需要"跨核借还"逻辑——而这正是 layer-2 现在做的事。per-core arena 等于把 layer-2 从"按需借还"变成"显式 owner + steal"，复杂度更高。
- **RDMA 约束**：slab 是单段连续 mmap 供 `ibv_reg_mr` 注册（`memory_pool.h:60-66`、`infiniband/memory.cc:192-193`）。per-core arena 只要仍切自同一段 mmap 即可（切的是逻辑区间，不是物理段），这点**可满足**，但要保证 `IndexOf` / `BaseAddr` 语义不变。
- **当前模型不绑核**：在 bthread M:N 下 worker 不绑核、ThreadSlot 是近似映射，"per-core arena"无法真正独占，收益打折。

**结论（建议）**：

- **近期保持 #7 不动**。它已经是 lock-free 且经过 tcmalloc 范式验证，不是当前瓶颈。
- 仅当 **profiling 实测 layer-2 Treiber 栈 CAS 成为热点**（如 `perf` 显示 `Require/Release` 的 atomic 占比显著）时，再做两步小改：① 把 `kNumCaches`/`kNumShards` 调到匹配实际 worker 数；② 若已绑核，给 layer-1 增加一个"严格 per-core、无 atomic_flag"的快路径，layer-2 作为 fallback 不变。这是渐进、可回退的，不是推倒重来。

### M5. 决策树（怎么给一个结构选改造方案）

```
是 bvar / 已 COW / 已 lock-free / 固定大小原子轮转 ？
  └─ 是 → 保持（#5/#6/#8/#14/#15/#16/#17）
否 → 该结构是否要求"同 key 看到同一份状态"（singleflight / 去重 / per-key LRU）？
  ├─ 是 → key 分片（M1/M2/M3）：shard = hash(key) & mask（#1/#2/#4）
  └─ 否（纯计数 / 无状态限流 / per-worker 缓冲）→ per-worker 实例（M2 的 fd 路 / #3）
中心分配器且已 lock-free ？
  └─ 是 → 保持，仅 profiling 驱动微调（M4，#7）
专题已覆盖 ？
  └─ 是 → 引用专题文档（#9/#10/#11）
```

---

## 文件结构

本横切策略落地时新增 / 改动的文件：

```
src/cache/iutil/
├── inflight_tracker.h          # 改：InflightTracker → 内部分片（保持类名与 Add/Remove 接口不变）
├── sharding.h                  # 新：统一分片基元（ShardIndex / alignas(64) Shard 宏 / kShardCount 选取）
└── buffer_pool.h               # 改：legacy BufferPool 标记 deprecated，迁移到 MemoryPool 底座

src/cache/common/
├── task_tracker.h              # 改：TaskTracker → 按 key 分片（DownloadTask 不动）
├── storage_client_pool.{h,cc}  # 不改（论证保持；可选：读路径换 RCU/原子指针，见兼容性）
└── slab_buffer.{h,cc}          # 不改（保持薄壳）

src/common/writemempool/
└── memory_pool.{h,cc}          # 不改（保持 lock-free；仅 profiling 驱动微调 kNumShards/kNumCaches）

src/cache/docs/cache-perf/
├──  01-shard-diskcache-lock.md  # 专题：磁盘 LRU 锁分片（本文引用）
├──  04-parallel-upload-pipeline.md# 专题：上传队列分片（本文引用）
└── 07-shared-state-sharding.md # 本文
```

> 设计取向：**对调用方零侵入**。`InflightTracker` / `TaskTracker` 改造均**保持现有类名、构造签名与 `Add/Remove`/`GetOrCreateTask/RemoveTask` 接口不变**，仅把内部单锁换成分片数组。所有 `local_block_cache.cc:96-97`、`node.cc:83` 等构造点和调用点不需修改。

---

## 实现步骤

按"风险从低到高、收益从高到低"排序，每步独立可合入、独立可回退：

1. **新增 `iutil/sharding.h` 基元**。抽出 `kShardCount` 选取（`>= worker 数`的最小 2 的幂）、`ShardIndex(key)`、`alignas(64)` Shard 约定、`static_assert` 2 的幂。这是后续所有改造的公共依赖，先落地、加单测。

2. **改造 `TaskTracker`（#4，收益最高、语义最干净）**。按 §M3 把 `tasks_` 拆成分片数组，`DownloadTask` 不动。这是下载热路径，singleflight 分片直接降低 `node.cc:327` 那把锁的争用。对照 `mem_cache` 写法，加单测验证"同 key 必同 shard、leader/follower 复用不变"。

3. **改造 cache/prefetch 两路 `InflightTracker`（#1）**。按 §M2 内部分片，`max_inflights` 等分（1024→每片 16）。构造点 `local_block_cache.cc:96-97`、`tier_block_cache.cc:71-73` 不变。

4. **处理 upload `InflightTracker`（#2）**。按 §风险对策选定方案（限定 shard 数 = `min(kShardCount, 32)` 或将限额下放给 04 文档队列），与 04 文档协同评审。

5. **评估盘 AIO `InflightTracker`（#3）**。先 profiling 单盘下 `inflight_` 锁是否真争用；若不显著则**保持**，并在矩阵中标注"已 per-disk，保持"。

6. **淘汰 legacy `BufferPool`（#18）**。确认调用方后迁移到 `MemoryPool` 底座或直接删除，消掉那把 condvar 单锁。

7. **`StorageClientPool`（#5）保持**，仅在 §兼容性 中记录"读路径可选 RCU 化"作为远期 backlog，不在本期动。

8. **`MemoryPool`（#7）仅挂 profiling**，不改代码；把"per-core arena 仅 profiling 驱动"写入 backlog。

9. **回归与压测**。每步合入后跑对应单测 + 一轮 io 压测，确认锁 profile（`bvar` 的 contention 指标 / `perf lock`）下降且功能无回归。

---

## 兼容性与灰度

- **接口零变更**：所有改造保持类名与公开方法签名，调用方与构造点不动，二进制行为对上层透明。
- **分片数可配置 + 灰度开关**：`kShardCount` 不写死常量，提供 `gflags`（如 `FLAGS_inflight_tracker_shards`、`FLAGS_task_tracker_shards`，默认 64）。**灰度策略**：将分片数设为 `1` 即退化为当前单锁行为，等价于"关闭分片"，可在生产先以 `1` 部署验证无回归，再逐步调大。这给了一个零代码改动的回退路径。
- **限额一致性**：`InflightTracker` 分片后全局限额变为"各片限额之和"，对 cache/prefetch（1024）等价；对 upload（32）需在 release note 注明实际全局上限的取整变化（见 §风险）。
- **`StorageClientPool` 远期 RCU**：若将来读路径要彻底去锁，可把 `clients_` 换成 `std::atomic<shared_ptr<const Map>>`（COW + 原子换指针，类比 #14 `RemoteCacheCluster` 的做法），读路径取一次指针即 lock-free，写路径整表复制。属可选项，**本期不做**。
- **`MemoryPool` 调参**：`kNumShards`/`kNumCaches`/`kRefillBatch` 若改为可配，需同步审计 RDMA 注册路径（单段 mmap 约束）不受影响。

---

## 风险与对策

| 风险 | 影响 | 对策 |
|---|---|---|
| **upload tracker（#2）限额 32 < shard 数，等分后限流失效** | 上传并发失控或形同无限 | shard 数取 `min(kShardCount, FLAGS_upload_stage_max_inflights)` 并每片 `ceil`；或把"全局限额"职责上移到 04 文档的分片上传队列，tracker 只保留去重。需与 04 文档协同定稿。 |
| **限额取整导致全局上限漂移** | 实际并发上限 ≠ 配置值（每片 `ceil` 后 ×shard 数会略大于原值） | 在文档/日志显式打印"有效全局上限 = per_shard × shards"；对限额敏感场景把 shard 数设为能整除 total 的值。 |
| **分片热点：key 分布倾斜** | 个别 shard 退化为单锁 | 用 `std::hash`（已具备良好雪崩）；filename 含 chunk/block 序号，天然分散；必要时换 `absl::Hash`。监控可在 `Dump` 暴露各 shard 计数观察倾斜。 |
| **伪共享：忘了 `alignas(64)`** | 分片不降反升（相邻 mutex 同 cache-line） | 模板强制 `alignas(64)`，加 `static_assert(sizeof(Shard) % 64 == 0)` 或在 code review checklist 固化。 |
| **singleflight 误按核分片** | 同 block 重复下载、去重击穿 | §M3 已明确：singleflight 永远按 key 路由，禁止按核分片；单测覆盖"两 worker 同 key 只 1 leader"。 |
| **过度分片浪费内存 / 启动开销** | 每个 `unordered_map`×64 的固定开销 | shard 数封顶 64；空 map 开销可忽略；`InflightTracker` 的 `busy_` 集合按需增长。 |
| **盲目给已 lock-free 的 MemoryPool 再加锁/再分片** | 复杂度上升、收益为负 | §M4 决策：保持，仅 profiling 驱动微调；禁止无数据支撑的 per-core arena 重写。 |
| **bthread 不绑核，"per-core" 名不副实** | 误以为已 share-nothing | 文档已澄清当前是 lock-striping；真 per-core 列为远期、需先有 worker 亲和支撑，改造矩阵的分片粒度已为此前向兼容。 |

---

## 测试方案

1. **单元测试（功能等价）**
   - `ShardedInflightTracker`：`Add` 同 key 返回 `Exist`、限额达到时 `Add` 阻塞、`Remove` 后唤醒；shard=1 时行为与原 `InflightTracker` 逐字节等价（黄金对照）。
   - `ShardedTaskTracker`：同 key 必同 shard、`GetOrCreateTask` 首次返回 `true` 其余 `false`、`RemoveTask` 后可重新成为 leader；并发两 bthread 同 key 仅一个 leader（singleflight 不变量）。
   - 对照现有 `tests/unit/cache/`（参照 `mem_cache` 已有测试）补 `sharding.h` 的 `ShardIndex` 分布/掩码测试。

2. **并发 / 压力测试**
   - 多 bthread（`./test.py -c <n>` 提高 smp）对分片结构高并发 `Add/Remove`、`GetOrCreate/Remove`，跑 ASan/TSan（`build/sanitize`）验证无数据竞争、无死锁。
   - singleflight 压测：N 个 bthread 抢同一 key，断言"实际下载次数 == 1"。

3. **锁争用回归（量化收益）**
   - 改造前后各跑一轮 io 压测（参照 CLAUDE.md 的 io_tester / 现有 cache 压测），对比：
     - `perf lock` / `perf record` 中目标锁的占比；
     - bthread mutex contention 相关 bvar（若有）；
     - 端到端 P99 延迟与吞吐。
   - 验收标准：目标结构锁争用显著下降、吞吐不降、P99 不升、功能无回归。

4. **灰度开关验证**
   - 以 `FLAGS_*_shards=1` 部署，确认与改造前完全一致（回退路径有效）；再调到 64 验证收益。

5. **构建 / 全量回归**
   - `ninja -C build/dev` 全量编译；`./test.py --mode dev --name 'inflight|task_tracker|mem_cache'` 跑相关单测；合入前在 `sanitize` 模式过一遍目标测试。
