# 分片磁盘缓存索引锁（消除 DiskCacheManager 全局大锁）

## 概述

本文档描述对 `DiskCacheManager` 的并发改造方案：把当前由**一把全局 `bthread::Mutex`** 串行化的磁盘缓存索引（cached blocks LRU + staging blocks 表 + 容量记账），按 block-key 哈希切分为 `N` 个互相独立的 shard，每个 shard 拥有**独立的锁、独立的 LRU、独立的 staging 表与独立的容量记账**。不同 key 的 `Add/Delete/Exist` 落在不同 shard 上，从而不再互相阻塞，消除热点路径上的锁竞争。

改造完全参照同目录下已落地的 `MemCache`（`src/cache/local/mem_cache.{h,cc}`）分片范式：`std::array<Shard, kShardCount>` + 每 shard 一把锁 + `alignas(64)` 防伪共享。对外接口（`Add/Delete/Exist/StageFull/CacheFull/Start/Shutdown`）保持不变，调用方 `DiskCache` 无感知。

本方案是「缓存性能优化」项目的第一项，编号 `01`。

---

## 背景与动机

`DiskCacheManager` 负责磁盘缓存的**元数据索引**与**容量管理**：它不碰块文件数据本身（块文件读写由 `LocalFileSystem` 完成），只维护「哪些 block 在缓存里、占多少空间、什么时候过期、满了删哪些」。它被 `DiskCache` 以 `std::make_shared<DiskCacheManager>(...)` 持有（`src/cache/local/disk_cache.cc:65`），每个磁盘缓存目录（cache_index）一个实例。

当前所有索引操作都抢同一把 `mutex_`（`src/cache/local/disk_cache_manager.h:121`）：

| 路径 | 位置 | 持锁内容 |
|------|------|----------|
| `Add` | `disk_cache_manager.cc:127` | `std::lock_guard<bthread::Mutex> lk(mutex_)` 全程持有 |
| `Delete` | `disk_cache_manager.cc:157` | 同上 |
| `Exist` | `disk_cache_manager.cc:165` | 同上，且源码注释 `// FIXME: lock contention`（`disk_cache_manager.cc:164`） |
| `CheckFreeSpace` → `CleanupFull` | `disk_cache_manager.cc:218` | 后台线程清理时持锁遍历淘汰 |
| `CleanupExpire` | `disk_cache_manager.cc:268` | 后台线程过期清理时持锁遍历 |

这把锁同时保护三份状态：

- `cached_blocks_`：`LRUCache`（`disk_cache_manager.h:126`），其底层 `LRUCache`（`src/cache/local/lru_cache.cc`）**本身非线程安全**——`Add/Get/Delete/Evict` 都直接操作侵入式双向链表（`active_`/`inactive_`）和哈希表，没有任何内部同步，完全依赖外层这把锁串行化。
- `staging_blocks_`：`std::unordered_map<std::string, CacheValue>`（`disk_cache_manager.h:127`），记录尚未上传到远端存储、**绝对不能被淘汰删除**的 stage block（删了会导致本地与远端都拿不到块，引发 IO error，见 `disk_cache_manager.h:114-119` 注释）。
- `used_bytes_`：全盘已用字节记账（`disk_cache_manager.h:122`），由 `UpdateUsage`（`disk_cache_manager.cc:339`）维护，并驱动 `Add` 内部的容量触发淘汰（`disk_cache_manager.cc:143`）。

**问题**：`Exist` 在每次读缓存命中判断时都会被调用，是高频读路径；`Add` 在每次 stage/cache 写入时调用，是高频写路径；后台 `CheckFreeSpace`/`CleanupExpire` 每秒都会持锁遍历淘汰。在高并发块读写（尤其大量小块、warmup 场景）下，这把全局锁成为磁盘缓存吞吐的瓶颈。源码已自带 `// FIXME: lock contention` 标注，明确这是已知待优化点。

**已有范式**：`MemCache`（`src/cache/local/mem_cache.h:80`）面对完全相同的「LRU + 索引 + 容量记账」结构，已采用 `kShardCount = 32` 的分片设计（`mem_cache.h:82`），每个 `Shard`（`mem_cache.h:123`）`alignas(64)` 防止相邻 shard 落在同一 cache line 上产生伪共享，`ShardIndex` 用 `std::hash<std::string>{}(key) & (kShardCount - 1)`（`mem_cache.h:86`）定位。本方案把同一范式复制到磁盘缓存索引。

---

## 当前实现分析

### 数据结构（现状）

```cpp
// src/cache/local/disk_cache_manager.h
class DiskCacheManager {
  std::atomic<bool> running_;
  bthread::Mutex mutex_;                 // 一把全局锁
  uint64_t used_bytes_;                  // 全盘已用字节
  const uint64_t capacity_bytes_;        // 全盘容量
  std::atomic<bool> stage_full_;         // 由磁盘空间巡检置位（非索引锁保护）
  std::atomic<bool> cache_full_;
  LRUCacheUPtr cached_blocks_;           // 全局一份 LRU
  std::unordered_map<std::string, CacheValue> staging_blocks_;  // 全局一份 staging
  utils::TaskThreadPoolUPtr thread_pool_;
  DiskCacheLayoutSPtr layout_;
  bthread::ExecutionQueueId<ToDel> queue_id_;  // 异步删块文件队列
  DiskCacheManagerMetricsUPtr vars_;
};
```

### 关键行为（现状）

1. **三相状态机**（`BlockPhase`，`disk_cache_manager.h:73`）：
   - `kStaging`：块刚写入本地、尚未上传。进 `staging_blocks_`，`used_bytes_ += size`。
   - `kUploaded`：上传完成。把条目从 `staging_blocks_` **搬到** `cached_blocks_`（`disk_cache_manager.cc:132-137`），注意此处**不改 `used_bytes_`**（字节早在 staging 阶段已记账），只是状态迁移。
   - `kCached`（直接缓存，未经 stage）：直接进 `cached_blocks_`，`used_bytes_ += size`。

2. **容量触发淘汰**（`Add` 内，`disk_cache_manager.cc:143`）：当 `used_bytes_ >= capacity_bytes_`，算出 `want_free_bytes = used - capacity*0.95`、`want_free_files = size*0.05`，调 `CleanupFull` 从 LRU 尾部淘汰，淘汰出的 `CacheItems` 通过 `bthread::execution_queue` 异步删块文件（`DeleteBlocks`，`disk_cache_manager.cc:304`）。

3. **磁盘空间巡检**（`CheckFreeSpace`，后台线程，`disk_cache_manager.cc:180`）：每秒 `statfs`，根据剩余空间比例置 `cache_full_`/`stage_full_`（原子，不走索引锁），空间紧张时持锁 `CleanupFull` 主动腾空间。

4. **过期清理**（`CleanupExpire`，后台线程，`disk_cache_manager.cc:254`）：每 `cache_cleanup_expire_interval_ms` 持锁从 LRU 头部扫描，`atime + expire_s <= now` 的淘汰，单轮最多检查 `1e3` 个（`disk_cache_manager.cc:270`）。

### 改造的约束点

- **staging block 永不被淘汰**：`CleanupFull`/`CleanupExpire` 只对 `cached_blocks_` 做 `Evict`，从不碰 `staging_blocks_`。分片后此语义必须保持——staging 与 cached 必须落在**同一 shard**，否则 `kUploaded` 的搬运会跨 shard 死锁或竞态。
- **`used_bytes_` 全盘语义**：容量是全盘配额（`capacity_bytes_`），不是每文件配额。分片后需要决定「每 shard 配额」还是「全局原子记账」。
- **删块文件异步队列是全局的**：`queue_id_`/`HandleTask`/`DeleteBlocks` 不碰索引、只做 `unlink`，可保持全局单队列，无需分片。

---

## 设计方案

### 总体思路

把 `cached_blocks_` + `staging_blocks_` + `used_bytes_` 三者一起按 key 哈希切进 `kShardCount` 个 `Shard`。**关键不变量：同一个 block-key 的 staging 条目与 cached 条目永远落在同一个 shard**——因为 `ShardIndex` 只依赖 key 本身，`kStaging` 和后续 `kUploaded`/`kCached` 用的是同一个 `key.Filename()`，天然同 shard。于是三相状态机的搬运、容量记账、淘汰全部在单个 shard 锁内闭环完成。

容量配额采用**每 shard 配额 + 全局原子兜底**的混合方案（见下文「容量配额」一节），既能在 shard 锁内独立判断是否触发淘汰，又能让监控/`CacheFull` 看到全盘真实用量。

### 数据结构

```cpp
// src/cache/local/disk_cache_manager.h
class DiskCacheManager {
 public:
  static constexpr size_t kShardCount = 32;  // 与 MemCache 对齐，必须是 2 的幂
  static_assert((kShardCount & (kShardCount - 1)) == 0,
                "kShardCount must be a power of 2");

  // 与 MemCache::ShardIndex 一致：std::hash(filename) 低位取模
  static size_t ShardIndex(const std::string& filename) {
    return std::hash<std::string>{}(filename) & (kShardCount - 1);
  }

 private:
  // Cache-line 对齐，避免相邻 shard 伪共享（参照 mem_cache.h:123）
  struct alignas(64) Shard {
    mutable bthread::Mutex mutex;
    uint64_t used_bytes{0};                 // 本 shard 已用字节（含 staging + cached）
    LRUCacheUPtr cached_blocks;             // 本 shard 独立 LRU
    std::unordered_map<std::string, CacheValue> staging_blocks;  // 本 shard staging
  };

  Shard& GetShard(const std::string& filename) {
    return shards_[ShardIndex(filename)];
  }

  std::atomic<bool> running_;
  const uint64_t capacity_bytes_;        // 全盘容量（不变）
  const uint64_t shard_capacity_bytes_;  // = ceil(capacity_bytes_ / kShardCount)
  std::atomic<bool> stage_full_;
  std::atomic<bool> cache_full_;
  std::atomic<int64_t> total_used_bytes_; // 全盘已用字节（原子，仅用于监控 & CacheFull 判断）

  std::array<Shard, kShardCount> shards_;

  utils::TaskThreadPoolUPtr thread_pool_;
  DiskCacheLayoutSPtr layout_;
  bthread::ExecutionQueueId<ToDel> queue_id_;  // 保持全局单删块队列
  DiskCacheManagerMetricsUPtr vars_;
};
```

说明：
- 原 `mutex_` / `used_bytes_` / `cached_blocks_` / `staging_blocks_` 四个成员**整体下沉**到 `Shard`。
- 新增 `shard_capacity_bytes_`（构造期算好）与 `total_used_bytes_`（原子，跨 shard 汇总）。
- `LRUCache` 不改一行：它本就被外层锁保护，现在改由 shard 锁保护，每 shard 一个独立实例即可。

### 架构图

```
                           DiskCacheManager
                          (capacity_bytes_, total_used_bytes_ : atomic)
                                     |
        key = handle.Filename()      |  ShardIndex(key) = hash(key) & (N-1)
                                     v
   +---------+---------+---------+ ... +---------+   std::array<Shard, 32>
   | Shard 0 | Shard 1 | Shard 2 |     | Shard31 |
   +---------+---------+---------+ ... +---------+
   | mutex   | mutex   | mutex   |     | mutex   |   <- 每 shard 独立锁
   | LRU     | LRU     | LRU     |     | LRU     |   <- 每 shard 独立 cached LRU
   | staging | staging | staging |     | staging |   <- 每 shard 独立 staging map
   | used    | used    | used    |     | used    |   <- 每 shard 独立字节记账
   +----+----+----+----+----+----+     +----+----+
        |         |         |               |
        +---------+----+----+---------------+
                       |  淘汰出的 CacheItems
                       v
            bthread::execution_queue (全局单队列, queue_id_)
                       |
                       v
               DeleteBlocks -> unlink 块文件   (不碰索引, 无锁)

   后台线程 (thread_pool_, 2 worker):
     CheckFreeSpace  : 每秒 statfs -> 置 cache_full_/stage_full_(atomic)
                       -> 若空间紧张, 逐 shard 加锁 CleanupFull
     CleanupExpire   : 每 interval, 逐 shard 加锁扫描过期条目
```

### 关键函数签名与实现要点

#### 1. 构造与初始化

```cpp
DiskCacheManager::DiskCacheManager(uint64_t capacity, DiskCacheLayoutSPtr layout)
    : running_(false),
      capacity_bytes_(capacity),
      shard_capacity_bytes_((capacity + kShardCount - 1) / kShardCount),  // 向上取整
      total_used_bytes_(0),
      layout_(std::move(layout)),
      queue_id_({0}),
      vars_(std::make_unique<DiskCacheManagerMetrics>(layout_->CacheIndex())) {
  for (auto& shard : shards_) {
    shard.cached_blocks = std::make_unique<LRUCache>();
  }
  Init();
}

// Init() / 重启清理：逐 shard 持锁清空（替换原全局清空）
void DiskCacheManager::Init() {
  total_used_bytes_.store(0);
  stage_full_ = false;
  cache_full_ = false;
  for (auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    shard.cached_blocks->Clear();
    shard.staging_blocks.clear();
    shard.used_bytes = 0;
  }
}
```

#### 2. `Add`（对外签名不变）

```cpp
void DiskCacheManager::Add(const CacheKey& key, const CacheValue& value,
                           BlockPhase phase) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);

  bool need_cleanup = false;
  uint64_t want_free_bytes = 0, want_free_files = 0;
  {
    BAIDU_SCOPED_LOCK(shard.mutex);
    if (phase == BlockPhase::kStaging) {
      shard.staging_blocks.emplace(filename, value);
      UpdateUsageLocked(shard, 1, value.size);
      vars_->stage_blocks << 1;
    } else if (phase == BlockPhase::kUploaded) {
      auto iter = shard.staging_blocks.find(filename);
      CHECK(iter != shard.staging_blocks.end());     // staging 与 cached 同 shard, 必中
      shard.cached_blocks->Add(key, iter->second);   // 仅状态迁移, 不动 used_bytes
      shard.staging_blocks.erase(iter);
      vars_->stage_blocks << -1;
    } else {  // kCached
      shard.cached_blocks->Add(key, value);
      UpdateUsageLocked(shard, 1, value.size);
    }

    // 每 shard 配额触发淘汰
    if (shard.used_bytes >= shard_capacity_bytes_) {
      want_free_bytes = shard.used_bytes - shard_capacity_bytes_ * 0.95;
      want_free_files = shard.cached_blocks->Size() * 0.05;
      CleanupFullLocked(shard, want_free_bytes, want_free_files);  // 在持锁内完成 Evict
    }
  }
}
```

**要点**：
- shard 锁只保护索引内存操作，`execution_queue_execute`（异步删块文件入队）可放在锁外或锁内（它只入队，开销极小，保持锁内更简单）。
- `UpdateUsageLocked` 同时更新 `shard.used_bytes`（已持锁，普通加法）和 `total_used_bytes_`（原子），并打 bvar。
- 触发淘汰阈值从「全盘 `capacity_bytes_`」改为「本 shard `shard_capacity_bytes_`」，语义见「容量配额」。

#### 3. `Delete` / `Exist`（对外签名不变）

```cpp
void DiskCacheManager::Delete(const CacheKey& key) {
  auto& shard = GetShard(key.Filename());
  BAIDU_SCOPED_LOCK(shard.mutex);
  CacheValue value;
  if (shard.cached_blocks->Delete(key, &value)) {
    UpdateUsageLocked(shard, -1, -static_cast<int64_t>(value.size));
  }
}

// FIXME 消除：只抢单个 shard 锁
bool DiskCacheManager::Exist(const CacheKey& key) {
  auto filename = key.Filename();
  auto& shard = GetShard(filename);
  BAIDU_SCOPED_LOCK(shard.mutex);
  CacheValue value;
  return shard.cached_blocks->Get(key, &value) ||
         shard.staging_blocks.find(filename) != shard.staging_blocks.end();
}
```

#### 4. 容量记账

```cpp
// 假设已持有 shard.mutex
void DiskCacheManager::UpdateUsageLocked(Shard& shard, int64_t n,
                                         int64_t used_bytes) {
  shard.used_bytes += used_bytes;                    // 锁内, 普通加减
  total_used_bytes_.fetch_add(used_bytes, std::memory_order_relaxed);
  vars_->used_bytes.set_value(total_used_bytes_.load(std::memory_order_relaxed));
  vars_->cache_blocks << n;
  vars_->cache_bytes << used_bytes;
}
```

#### 5. 后台清理：逐 shard 加锁

```cpp
// CleanupFull: 假设已持有 shard.mutex（从 Add 或 CheckFreeSpace 调入）
void DiskCacheManager::CleanupFullLocked(Shard& shard, uint64_t want_free_bytes,
                                         uint64_t want_free_files) {
  uint64_t freed_bytes = 0, freed_files = 0;
  auto to_del = shard.cached_blocks->Evict([&](const CacheValue& value) {
    if (freed_bytes >= want_free_bytes && freed_files >= want_free_files) {
      return FilterStatus::kFinish;
    }
    freed_bytes += value.size;
    freed_files++;
    UpdateUsageLocked(shard, -1, -static_cast<int64_t>(value.size));
    return FilterStatus::kEvictIt;
  });
  if (!to_del.empty()) {
    CHECK_EQ(0, bthread::execution_queue_execute(
                    queue_id_, ToDel{std::move(to_del), "cache full"}));
  }
}

// CheckFreeSpace 内空间紧张时: 把全盘 want_free 平摊到各 shard, 逐 shard 加锁清理
void DiskCacheManager::CleanupAllShardsFull(uint64_t want_free_bytes,
                                            uint64_t want_free_files) {
  uint64_t per_shard_bytes = (want_free_bytes + kShardCount - 1) / kShardCount;
  uint64_t per_shard_files = (want_free_files + kShardCount - 1) / kShardCount;
  for (auto& shard : shards_) {
    BAIDU_SCOPED_LOCK(shard.mutex);
    CleanupFullLocked(shard, per_shard_bytes, per_shard_files);
  }
}

// CleanupExpire: 逐 shard 加锁扫描, 每 shard 限额检查避免长持锁
void DiskCacheManager::CleanupExpire() {
  CHECK_RUNNING("Disk cache manager");
  while (running_.load(std::memory_order_relaxed)) {
    auto cache_expire_s = FLAGS_cache_expire_s;
    if (cache_expire_s == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(3));
      continue;
    }
    auto now = iutil::TimeNow();
    for (auto& shard : shards_) {
      CacheItems to_del;
      uint64_t num_checks = 0;
      {
        BAIDU_SCOPED_LOCK(shard.mutex);
        to_del = shard.cached_blocks->Evict([&](const CacheValue& value) {
          if (++num_checks > 1e3 / kShardCount + 1) {   // 单 shard 限额, 总量持平
            return FilterStatus::kFinish;
          } else if (value.atime + cache_expire_s > now) {
            return FilterStatus::kSkip;
          }
          UpdateUsageLocked(shard, -1, -static_cast<int64_t>(value.size));
          return FilterStatus::kEvictIt;
        });
      }
      if (!to_del.empty()) {
        CHECK_EQ(0, bthread::execution_queue_execute(
                        queue_id_, ToDel{std::move(to_del), "cache expired"}));
      }
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(FLAGS_cache_cleanup_expire_interval_ms));
  }
}
```

### 容量配额：每 shard 配额 vs 全局原子

两种记账目标并存，方案选择「**每 shard 软配额触发淘汰 + 全局原子用于巡检/监控**」：

| 维度 | 方案 | 说明 |
|------|------|------|
| 触发淘汰的阈值判断 | **每 shard 配额** `shard_capacity_bytes_` | `Add` 内在 shard 锁内即可判断，无需读全局，零额外竞争 |
| 全盘用量监控/`vars_->used_bytes` | **全局原子** `total_used_bytes_` | `UpdateUsageLocked` 内 `fetch_add`，给 bvar 和巡检读 |
| 磁盘真满判断（`cache_full_`/`stage_full_`） | **statfs 实测**（不变） | `CheckFreeSpace` 本就基于真实剩余空间，与逻辑配额解耦 |

**为何不用纯全局原子做淘汰阈值**：若 `Add` 内读全局 `total_used_bytes_` 判断是否超过 `capacity_bytes_`，则所有 shard 会争抢同一原子并可能同时触发淘汰风暴；每 shard 独立软配额把决策本地化，自然分散。

**淘汰语义的变化（需在文档与代码注释中明确）**：
- 原行为：全盘共用一条 LRU，淘汰的是**全盘最久未访问**的块。
- 新行为：每 shard 一条 LRU，淘汰的是**所在 shard 内最久未访问**的块。由于 key 哈希均匀，各 shard 用量与冷热分布在统计上趋同（`kShardCount=32`，单 shard 容量 = 全盘/32）。代价是淘汰不再严格全局 LRU——某个 shard 可能淘汰一个比另一 shard 保留块更新的块。对缓存命中率的影响在均匀哈希下可忽略（与 `MemCache` 已接受的语义一致）。
- 容量上界由「全盘单上界」变为「各 shard 上界之和」。因 `shard_capacity_bytes_` 向上取整，总配额 `= kShardCount * ceil(capacity/kShardCount) >= capacity`，最坏多出 `(kShardCount-1)` 字节，可忽略；真实磁盘满由 `CheckFreeSpace` 的 statfs 兜底，不会超卖磁盘。

### `StageFull` / `CacheFull`（不变）

二者读 `stage_full_`/`cache_full_` 原子，由 `CheckFreeSpace` 基于 statfs 设置，与索引锁无关，分片改造不触及。

---

## 文件结构

| 文件 | 改动 | 说明 |
|------|------|------|
| `src/cache/local/disk_cache_manager.h` | **改** | `mutex_`/`used_bytes_`/`cached_blocks_`/`staging_blocks_` 下沉为 `struct alignas(64) Shard`；新增 `kShardCount`/`ShardIndex`/`GetShard`/`shard_capacity_bytes_`/`total_used_bytes_`/`std::array<Shard,32> shards_`；新增 `UpdateUsageLocked`/`CleanupFullLocked`/`CleanupAllShardsFull` 私有方法声明 |
| `src/cache/local/disk_cache_manager.cc` | **改** | 重写 `Init`/`Add`/`Delete`/`Exist`/`CheckFreeSpace`/`CleanupFull`/`CleanupExpire`/`UpdateUsage` 为分片版本；构造函数初始化各 shard 的 `cached_blocks` |
| `src/cache/local/lru_cache.{h,cc}` | **不改** | 继续作为单线程结构，由 shard 锁保护 |
| `src/cache/local/disk_cache.{h,cc}` | **不改** | 仍 `std::make_shared<DiskCacheManager>(capacity, layout)`，接口不变 |
| `src/cache/local/mem_cache.{h,cc}` | **不改** | 仅作分片范式参照 |
| `tests/unit/cache/local/disk_cache_manager_test.cc` | **增/改** | 新增分片并发与配额单测（详见测试方案）；若已有该测试文件则扩充 |

无新增源文件；改动集中在 `disk_cache_manager.{h,cc}` 两文件。

---

## 实现步骤

1. **头文件改造**（`disk_cache_manager.h`）
   - 引入 `<array>`，加 `kShardCount` 常量与 `static_assert`、`ShardIndex`。
   - 定义 `struct alignas(64) Shard`，把四个旧成员搬入。
   - 新增 `GetShard`、`shard_capacity_bytes_`、`total_used_bytes_`、`shards_` 与 `*Locked` 私有方法声明，删除旧的 `mutex_`/`used_bytes_`/`cached_blocks_`/`staging_blocks_` 成员。

2. **构造函数与 `Init`**（`disk_cache_manager.cc`）
   - 初始化列表算 `shard_capacity_bytes_`、`total_used_bytes_(0)`；构造体内逐 shard `make_unique<LRUCache>()`。
   - `Init` 改为逐 shard 持锁清空 + 重置原子。

3. **改 `UpdateUsage` → `UpdateUsageLocked(Shard&, n, bytes)`**：先实现它，后续方法依赖。

4. **改 `Add`**：按 phase 操作对应 shard，shard 锁内完成三相迁移 + 配额触发 `CleanupFullLocked`。

5. **改 `Delete` / `Exist`**：定位 shard 后单锁操作；删除 `Exist` 上方 `// FIXME: lock contention` 注释（问题已解决）。

6. **改 `CleanupFull` → `CleanupFullLocked(Shard&, ...)`** 并新增 `CleanupAllShardsFull`。

7. **改 `CheckFreeSpace`**：空间紧张分支改调 `CleanupAllShardsFull`（把全盘 `want_free` 平摊）；日志里的 `used_bytes_` 改读 `total_used_bytes_`。

8. **改 `CleanupExpire`**：外层 `for` 逐 shard，单 shard 锁内 `Evict`，单 shard 检查上限设为 `1e3/kShardCount+1` 保持总检查量级。

9. **编译与现有单测回归**：
   ```bash
   ninja -C build/dev <disk_cache 相关 target>
   ./test.py --mode dev --name disk_cache
   ```

10. **补分片单测与压测**（见测试方案）。

---

## 兼容性与灰度

- **对外接口零变更**：`DiskCacheManager` 的 public 方法签名全部保持，`DiskCache` 不需要任何改动；磁盘上的块文件布局（`DiskCacheLayout`）、重启加载流程（`DiskCacheLoader`）均不受影响——索引是纯内存态，进程重启时由 `loader_` 重建。
- **bvar 监控指标保持**：`dingofs_disk_cache_<idx>_used_bytes`/`cache_blocks`/`cache_bytes`/`stage_blocks` 等名字与含义不变（`used_bytes` 由 `total_used_bytes_` 汇总驱动），现有监控面板无需调整。
- **灰度开关**：把 `kShardCount` 暴露为可配置 gflag（建议 `DEFINE_uint32(disk_cache_index_shards, 32, ...)`，要求 2 的幂，否则回退 1）。
  - `disk_cache_index_shards=1` 即退化为「单 shard」，行为等价于改造前的全局单锁（全盘单 LRU、全盘单配额），作为**安全回退档位**。
  - 线上可先以 `=1` 部署验证功能等价，再调到 `8/16/32` 观察竞争与命中率。
  - 注意该值仅在构造期生效，需重启进程切换；属于纯内存索引，重启无数据迁移成本。
- **回滚**：代码层面单 commit 可 `git revert`；运行期把 `disk_cache_index_shards` 调回 `1` 即等价回退到旧行为，无需回滚二进制。

---

## 风险与对策

| 风险 | 影响 | 对策 |
|------|------|------|
| staging 与 cached 跨 shard | `kUploaded` 找不到 staging 条目 → `CHECK` 失败 | `ShardIndex` 仅依赖 `key.Filename()`，同一 key 三相必然同 shard；单测覆盖 stage→upload→cache 全链路落同一 shard |
| 淘汰从全局 LRU 退化为 per-shard LRU | 命中率理论上略降 | 哈希均匀 + `kShardCount=32` 使各 shard 冷热趋同；压测对比命中率，提供 `shards=1` 回退档 |
| 每 shard 配额向上取整导致总配额略超 `capacity_bytes_` | 逻辑配额最多多 `kShardCount-1` 字节 | 可忽略；真实磁盘满由 `CheckFreeSpace` statfs 兜底，不超卖物理空间 |
| `CleanupExpire` 单轮遍历 32 个 shard，开销上升 | 后台 CPU 略增 | 单 shard 检查上限缩小为 `1e3/kShardCount+1`，总检查量级持平；逐 shard 取放锁，不长持任一锁 |
| `total_used_bytes_` 原子与各 shard `used_bytes` 之和短暂不一致 | 监控数值瞬时漂移 | 二者在同一 `UpdateUsageLocked` 内更新，仅原子与某 shard 字段非同一临界区，漂移在单次 `Add/Delete` 量级，可接受；`Dump` 类全量统计应逐 shard 加锁汇总（参照 `MemCache::Dump`，`mem_cache.cc:168`） |
| 伪共享 | 相邻 shard 同 cache line，分片收益被抵消 | `struct alignas(64) Shard`（参照 `mem_cache.h:123`） |
| `bthread::Mutex` 不可移动，`std::array<Shard>` 默认构造 | 编译/初始化问题 | `Shard` 用默认构造，`LRUCache` 在构造体内逐个 `make_unique` 赋值，不依赖聚合初始化 |

---

## 测试方案

### 单元测试（`tests/unit/cache/local/disk_cache_manager_test.cc`，Boost.Test）

1. **功能等价**
   - `Add(kStaging)` → `Exist`==true；`Add(kUploaded)` 后 staging 转 cached、`stage_blocks` 计数归零、`Exist` 仍 true。
   - `Add(kCached)` → `Delete` → `Exist`==false，`used_bytes` 正确回退。
   - staging block 在 `CleanupFull`/`CleanupExpire` 下**绝不**被淘汰。
2. **分片正确性**
   - 同一 key 的 staging 与 cached 落同一 shard（用 `DiskCacheManager::ShardIndex` 断言）。
   - 构造若干 key，断言 `ShardIndex` 分布跨多个 shard（哈希均匀性 sanity check）。
3. **容量与淘汰**
   - 单 shard 写满触发 `CleanupFull`，淘汰后 `used_bytes < shard_capacity`，被淘汰块入异步删队列。
   - 设极短 `cache_expire_s`，`CleanupExpire` 后过期 cached 块被清、staging 不动。
   - `total_used_bytes_` == Σ(各 shard `used_bytes`)（逐 shard 加锁汇总比对）。
4. **回退档**：`disk_cache_index_shards=1` 时行为与改造前一致（单 LRU、全盘配额）。
5. **并发安全**（多 bthread/线程并发 `Add/Delete/Exist` 同前缀 key），跑 ASan/TSan 验证无数据竞争：
   ```bash
   ./test.py --mode sanitize --name disk_cache_manager
   ```

### 压测 / 微基准（`tests/perf/`）

- 新增微基准：`P` 个 bthread 并发对 `M` 个随机 key 跑 `Exist`（读密集）与 `Add/Delete`（写密集）混合负载，分别在 `shards=1` 与 `shards=32` 下测 QPS 与 p99 延迟。
- 端到端：io_tester / warmup 场景下打满磁盘缓存，观测 `dingofs_disk_cache_*` bvar 与整体读吞吐。

### 对比指标（改造前 `shards=1` vs 改造后 `shards=32`）

| 指标 | 期望 |
|------|------|
| `Exist`/`Add` 操作 QPS | 显著上升（接近线性扩展，受 key 分布与核数限制） |
| 锁等待 / p99 延迟 | 明显下降 |
| 缓存命中率 | 基本持平（per-shard LRU 退化影响 < 1%，需实测确认） |
| `used_bytes` 监控值 | 与全盘真实用量一致，无漂移累积 |
| CleanupExpire 后台 CPU | 持平或微增，不出现长尾持锁 |

通过标准：功能单测全绿、Sanitizer 无 race、命中率回归在阈值内、并发吞吐相对 `shards=1` 有可观提升。
