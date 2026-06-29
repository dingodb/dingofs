# 后台上传流水线并行化（消除单 worker + 全局队列 + 轮询）

## 概述

DingoFS 的本地块缓存（`LocalBlockCache`）在 writeback 写入与磁盘重载（reload）后，会把"待上传到对象存储的 stage 块"投递给一个全局的 `BlockCacheUploader`，由它负责把数据从本地盘搬运到后端存储并清理 stage 文件。

当前实现存在三个串行化/低效瓶颈：

1. **单一 worker 线程**：整个进程只有一个 `std::thread` 在跑 `UploadWorker`，所有磁盘、所有 fs 的上传任务都从它这里出队。
2. **全局优先级队列**：所有 `DiskCache` 共用同一个 `PendingQueue`（`bthread::Mutex` 保护），成为全局争用点。
3. **10ms sleep 轮询**：队列为空时 worker `std::this_thread::sleep_for(10ms)` 轮询，既浪费 CPU，又给上传引入最高 10ms 的固有延迟。

本文档提出把上述结构改造为 **每磁盘（每 `DiskCache`）一条独立上传队列 + 多 worker + 事件驱动（条件变量替代 sleep 轮询）** 的并行流水线，并发度仍由 `InflightTracker` 控制，写回优先于重载（writeback > reload）的优先级语义完整保留。本方案与 `03`（消除上传时的回读盘）协同设计：`03` 让上传不再读盘，本文档让"调度"不再成为瓶颈，两者叠加才能把后台上传的吞吐打满。

本文档只涉及 `src/cache/local/block_cache_uploader.{h,cc}` 及其与 `disk_cache_loader.cc`、`local_block_cache.cc` 的接线，不改变上传的语义（去重、限流、失败重试、优先级）。

## 背景与动机

### 上传任务的两个来源

stage 块进入上传队列只有两个入口，对应 `BlockAttr::from` 的两个取值：

- **writeback**（`BlockAttr::kFromWriteback`，`src/cache/api/block_cache.h:45`）：上层写入开启 writeback 时，`LocalBlockCache` 先把块落到本地盘 stage 目录，再调用上传回调投递。投递点见 `src/cache/local/local_block_cache.cc:114-117`：

  ```cpp
  auto status = store_->Start(
      [this](BlockHandle handle, size_t length, BlockAttr block_attr) {
        uploader_->EnterUploadQueue(StageBlock(handle, length, block_attr));
      });
  ```

- **reload**（`BlockAttr::kFromReload`，`src/cache/api/block_cache.h:46`）：进程重启后，`DiskCacheLoader` 扫盘发现残留的 stage 文件，逐个回灌到同一个上传队列。投递点见 `src/cache/local/disk_cache_loader.cc:188-191`：

  ```cpp
  case BlockType::kStageBlock:
    manager_->Add(handle, CacheValue(file.size, file.atime), BlockPhase::kStaging);
    uploader_(handle, file.size, BlockAttr(BlockAttr::kFromReload, disk_id_));
    break;
  ```

  注意 reload 路径传入了 `disk_id_`（即该盘的 `uuid_`），并写进 `BlockAttr::store_id`（`src/cache/api/block_cache.h:54-59`）。这是块"真实所在磁盘"的权威标识。

### 单 uploader 服务多磁盘

`LocalBlockCache` 只构造一个 `BlockCacheUploader`（`src/cache/local/local_block_cache.cc:93-94`）：

```cpp
uploader_(std::make_shared<BlockCacheUploader>(store_, storage_client_pool_)),
```

而 `store_` 可能是一个 `DiskCacheGroup`，内部包含多块盘（多个 `DiskCache`，见 `src/cache/local/disk_cache_group.cc:56-68`）。也就是说：**N 块盘的所有上传任务，全部挤在 1 个全局队列、由 1 个 worker 线程出队。** 在多盘部署下，单盘的上传带宽天然被这条窄路径限制。

### 三个瓶颈的代码位置

- 单 worker 线程：`src/cache/local/block_cache_uploader.cc:166`
  ```cpp
  thread_ = std::thread(&BlockCacheUploader::UploadWorker, this);
  ```
- 全局优先级队列（`bthread::Mutex` + 按 `from` 分桶的优先级队列）：`src/cache/local/block_cache_uploader.cc:81-141`
- 空队列 10ms sleep 轮询：`src/cache/local/block_cache_uploader.cc:195-200`
  ```cpp
  while (IsRunning()) {
    auto sblocks = pending_queue_->Pop();
    if (sblocks.empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }
    for (const auto& sblock : sblocks) {
      AsyncUpload(sblock);
    }
  }
  ```
- 并发由 `InflightTracker tracker_` 控制（上限默认 32，`FLAGS_upload_stage_max_inflights`，去重 + 限流）：`src/cache/local/block_cache_uploader.cc:149-150`、`src/cache/iutil/inflight_tracker.h:37-71`
- 每个 sblock 真正上传时 `RunInBthread` 起一个新 bthread：`src/cache/local/block_cache_uploader.cc:225`

### 动机

1. **吞吐**：多盘并行上传时，单 worker + 全局队列让有效并发被人为压低；尤其重启后大批量 reload 场景，恢复时间被串行调度拖长。
2. **延迟**：10ms 轮询给每个 writeback 块的上传起步引入 0~10ms 抖动；在小块、高频写场景下显著。
3. **CPU 空转**：空闲期 worker 仍每 10ms 唤醒一次、抢一次 `bthread::Mutex`、扫一遍 map。
4. **可扩展性**：未来加盘时上传能力不随盘数线性增长。

## 当前实现分析

### 数据结构与调度

`PendingQueue`（`block_cache_uploader.cc:79-141`）的核心是 `std::unordered_map<BlockFrom, Segments<StageBlock>>`：按来源（writeback/reload）分桶，每个桶是一个 `Segments`（分段 vector，支持"压一个、批量弹一段"）。`Pop()` 按固定优先级 `{kFromWriteback, kFromReload, kFromUnknown}` 取第一个非空桶的一整段返回（`block_cache_uploader.cc:98-116`）。所有 `Push`/`Pop`/`Size`/`Stat` 都在同一把 `bthread::Mutex mutex_` 下。

### 上传执行链路

```
EnterUploadQueue(sblock)            // 生产者：writeback / reload
   └─> PendingQueue::Push           // 加锁入队，按 from 分桶

UploadWorker (单线程)               // 消费者
   while running:
     sblocks = PendingQueue::Pop()  // 取一整段（最多 kSegmentSize=100）
     if empty: sleep(10ms); continue
     for sblock in sblocks:
       AsyncUpload(sblock)

AsyncUpload(sblock)
   ├─ tracker_->Add(key)            // 去重(Exist即跳过) + 限流(满32则wait)
   ├─ RunInBthread([...]{           // 每块起一个 bthread
   │     DoUpload(sblock)           //   Load(读盘) -> Put(存储) -> RemoveStage
   │     OnComplete(sblock, status) //   失败则 sleep 100ms 后重新入队
   │     tracker_->Remove(key)      //   释放名额，唤醒等待者
   │  })
   └─ joiner_->BackgroundJoin(tid)  // 后台回收 bthread
```

关键点：

- **真正的并发其实发生在 bthread 层**：`AsyncUpload` 里 `RunInBthread` 已经为每块起独立 bthread（`block_cache_uploader.cc:225`），`tracker_` 把同时在飞的上传数限到 32。所以"单 worker 线程"并不直接限制上传并发度——它限制的是**出队与派发的速率**，以及**所有盘共享同一调度路径**。
- **`tracker_->Add` 可能阻塞**：当在飞数达到 32，`Add` 在 `cond_.wait` 上挂起（`inflight_tracker.h:48-49`）。由于这是在**单 worker 线程**里同步调用的，一旦阻塞，**整条出队流水线停摆**——即便此时别的盘的存储后端是空闲的，也派发不出新任务。这是当前架构最隐蔽、也最值得修的串行点。
- **失败重试**：`OnComplete`（`block_cache_uploader.cc:236-260`）对非 `ok/NotFound/CacheDown` 的错误 `bthread_usleep(100ms)` 后 `EnterUploadQueue` 重新入队，并在重入前后做 running 状态二次校验。
- **`DoUpload` 会回读本地盘**：`store_->Load(...)`（`block_cache_uploader.cc:262-296`）——这正是 `03` 文档要消除的开销，本文档对此不重复设计，但拓扑上需要与之兼容。

### 一句话结论

当前架构把"调度（出队/派发/限流等待）"全部塞进**一条线程 + 一把全局锁**，使其无法随盘数扩展，并以 10ms 轮询换取实现简单。瓶颈不在 bthread 并发上限，而在**派发路径的串行化**与 `tracker_->Add` 阻塞导致的**队头阻塞**。

## 设计方案

### 总体思路

1. **分片**：把全局 `PendingQueue` 拆成"**每 `DiskCache` 一条队列**"。块在入队时按其归属磁盘（writeback 用一致性哈希、reload 用 `store_id`）路由到对应队列。
2. **多 worker**：每条盘队列配一个独立 worker（线程或常驻 bthread），互不阻塞。
3. **事件驱动**：用 `bthread::ConditionVariable` 替代 10ms sleep——入队 `notify`，worker 在空队列时 `wait`，shutdown 时广播唤醒。
4. **并发控制**：`InflightTracker` 的语义保留。提供两种粒度（见下"并发控制粒度"），默认每盘一个 tracker，使限流真正落到"每块盘的存储吞吐"上。
5. **优先级保留**：每条盘队列内部仍按 `writeback > reload` 优先弹出，复用现有 `Segments` + 按 `from` 分桶逻辑。

### 线程 / 队列拓扑

改造前（现状）：

```
 writeback ─┐
            ├─> [ 全局 PendingQueue (bthread::Mutex) ]
 reload  ───┘            │
                         │  Pop()  (单 worker, 空则 sleep 10ms)
                         ▼
                   UploadWorker(thread)
                         │  for each: AsyncUpload
                         ▼
                   tracker_ (max 32, 全局)
                         │  RunInBthread (每块一 bthread)
                         ▼
              DoUpload: Load(盘) -> Put(存储) -> RemoveStage
```

改造后（每盘独立流水线 + 事件驱动）：

```
                    ┌─────────────── Router (EnterUploadQueue) ───────────────┐
 writeback(hash) ─> │  store_id = (from==reload ? attr.store_id               │
 reload(store_id)─> │              : group->GetStore(handle)->Id())           │
                    └───────────────┬───────────────────┬────────────────────┘
                                    │                   │
                  ┌─────────────────▼──┐     ┌──────────▼─────────────────┐
   DiskCache A    │ PendingQueue_A     │     │ PendingQueue_B   DiskCache B│
                  │  wb-bucket         │     │  wb-bucket                  │
                  │  reload-bucket     │     │  reload-bucket              │
                  │  cond_ + mutex_    │     │  cond_ + mutex_             │
                  └─────────┬──────────┘     └──────────┬──────────────────┘
                  Worker_A (wait/notify)      Worker_B (wait/notify)
                  Pop() 优先 wb > reload       Pop() 优先 wb > reload
                            │                            │
                  tracker_A (max=M)            tracker_B (max=M)
                  RunInBthread(每块)           RunInBthread(每块)
                            ▼                            ▼
              DoUpload: [03后]内存直传->Put     DoUpload: ...->Put
                  joiner_ (可共享/可每盘)       joiner_
```

要点：

- 每盘一条 `PendingQueue`（内部结构不变，仍按 `from` 分桶 + 优先级弹出）、一个 `Worker`、一个 `InflightTracker`。
- Router 只做"块 → 盘"的映射，O(1)（一致性哈希查一次或直接用 `store_id`）。
- `BthreadJoiner joiner_` 可全局共享一个（它本身是一条 `ExecutionQueue`，线程安全），无需每盘一个；这样不增加额外线程。

### 块到盘的路由

路由必须与 `DiskCacheGroup` 的现有定位逻辑完全一致，否则会出现"队列分到 A 盘但数据在 B 盘"。`DiskCacheGroup` 现有定位规则（`disk_cache_group.cc:111-153,183-205`）：

- `block_attr.store_id` 非空 → `GetStore(store_id)`（reload 块走这里，权威）。
- 否则 → `GetStore(handle)`（一致性哈希 `chash_->Lookup(handle.Id())`，writeback 块走这里）。

因此 Router 复用同一规则计算目标盘 id：

```cpp
std::string BlockCacheUploader::RouteDiskId(const StageBlock& sblock) const {
  const auto& store_id = sblock.block_attr.store_id;
  if (!store_id.empty()) {
    return store_id;                       // reload: 权威 store_id
  }
  return store_->LocateDiskId(sblock.handle);  // writeback: 一致性哈希
}
```

为此需要给 `CacheStore` 增加一个轻量定位接口（只返回盘 id，不做 I/O），由 `DiskCacheGroup` 实现为 `GetStore(handle)->Id()`，`DiskCache` 实现为返回自身 `Id()`，`MemCache` 返回固定单 id。这样 Router 无需感知 group/单盘差异。

> 边界：`MemCache`（`cache_store == "memory"`）只有一条"虚拟盘"，退化为单队列 + 单 worker，与现状行为一致，无回归。

### 事件驱动（条件变量替代轮询）

给每盘 `PendingQueue` 增加条件变量，把 `block_cache_uploader.cc:195-200` 的 sleep 轮询改为阻塞等待：

```cpp
// 入队侧
void PendingQueue::Push(const StageBlock& sblock) {
  std::unique_lock<bthread::Mutex> lk(mutex_);
  /* ... 原分桶逻辑不变 ... */
  not_empty_.notify_one();           // 唤醒该盘 worker
}

// 出队侧：阻塞直到有数据或被要求停止
std::vector<StageBlock> PendingQueue::PopBlocking(std::atomic<bool>& running) {
  std::unique_lock<bthread::Mutex> lk(mutex_);
  while (Empty_() && running.load(std::memory_order_relaxed)) {
    not_empty_.wait(lk);             // 不再 sleep 10ms
  }
  if (!running.load(std::memory_order_relaxed)) return {};
  return PopLocked_();               // 复用原优先级弹出
}
```

worker 主循环：

```cpp
void BlockCacheUploader::UploadWorker(DiskUploader* d) {
  WaitStoreReady();
  while (IsRunning()) {
    auto sblocks = d->queue->PopBlocking(running_);
    for (const auto& sblock : sblocks) {
      AsyncUpload(d, sblock);        // tracker / RunInBthread 逻辑不变
    }
  }
}
```

Shutdown 时：`running_ = false` 后，对每盘 `PendingQueue` 调一次 `not_empty_.notify_all()`（广播），让阻塞中的 worker 立即醒来退出，再 `join`。

> 注意 `bthread::ConditionVariable` 已在 `InflightTracker` 中使用（`inflight_tracker.h:26,70`），与 `bthread::Mutex` 配套、可跨 bthread/pthread 唤醒，无需引入新依赖。

### 并发控制粒度

`InflightTracker` 的两个职责拆开看：

- **去重**（`busy_` 集合，按 `key` 即 `handle.Filename()`）：必须保证同一块不被两个 worker 同时上传。由于路由是**确定性**的（同一块永远映射到同一盘 → 同一队列 → 同一 worker），跨盘不会撞到同一块，因此**每盘一个 tracker 即可完成去重**，无需全局集合。reload 与 writeback 可能产生同一 key 吗？同一块要么在某盘 stage（reload 来源），要么由 hash 落到某盘（writeback），两者最终都落到该块所属那块盘的同一队列，去重仍由该盘 tracker 覆盖。
- **限流**（`inflights_ <= max_inflights_`）：现状是全局 32。每盘一个 tracker 后，语义变成"**每盘最多 M 个在飞上传**"。这更贴合物理现实——限流的目的是别把单块盘/单存储连接打爆。

并发上限参数化：

- 新增 `FLAGS_upload_stage_max_inflights_per_disk`（默认沿用 32 或调小，见兼容性章节），每盘 tracker 用它构造。
- 保留 `FLAGS_upload_stage_max_inflights` 作为"全局封顶"的可选项（默认 0 = 不封顶）。若设置，则在每盘 tracker 之上再叠加一个共享的全局 tracker，先过盘内、再过全局，避免 N 盘 × M 把存储后端整体打爆。默认不开启，保持每盘独立的简单模型。

### 队列改 MPSC 无锁（可选项）

每盘队列天然是 **MPSC**：多个生产者（writeback 的多个上层 bthread、reload 线程）→ 单个消费者（该盘 worker）。这正是无锁 MPSC 队列的理想场景，可作为后续优化：

- **方案 A（默认，本期落地）**：`bthread::Mutex` + `bthread::ConditionVariable`。盘队列分片后，单盘锁争用已远低于原全局锁；实现简单、与现有 `Segments`/优先级逻辑零改动。**推荐先落地此方案**。
- **方案 B（可选，无锁）**：用 `bthread::ExecutionQueue<StageBlock>`（项目已在 `BthreadJoiner` 中使用，见 `bthread.cc:79-104`）。`execution_queue_execute` 是无锁 MPSC 入队，`HandleTid` 风格的回调即"单消费者"批量处理，天然事件驱动（无空轮询、无 sleep），且 `use_pthread`/bthread 可选。
  - 代价：ExecutionQueue 是 **FIFO**，无法直接表达 `writeback > reload` 优先级。两种折中：
    1. 每盘开**两条** ExecutionQueue（wb 一条、reload 一条），消费侧优先抽干 wb 队列——优先级用"两条 FIFO + 消费侧选择"表达。
    2. 仍用一条 ExecutionQueue，但消费回调把任务暂存进盘内的优先级 `Segments`，再按优先级派发——等于把锁换到回调里，收益打折。
  - 结论：**优先级是硬约束**，无锁化收益主要在"消除空轮询"，而方案 A 用条件变量已经消除了空轮询。因此 MPSC 无锁列为**可选优化**，建议在方案 A 稳定、且 profiling 证明单盘锁仍是热点后，再用"方案 B + 两条 FIFO"实现。

### 与 03（消除回读盘）的协同

`03` 改造 `DoUpload`：上传时不再 `store_->Load` 回读本地盘，而是复用 writeback 时已在内存的数据（writeback 路径携带 buffer，reload 路径仍需读盘一次）。两者关系：

- `03` 缩短**单个上传任务**的关键路径（去掉一次盘 I/O）；本文档拓宽**调度通道**（多 worker + 每盘队列）。
- `03` 让 `DoUpload` 更快返回 → 在飞名额释放更快 → 本文档的 worker 派发频率需求更高 → 事件驱动 + 多 worker 正好承接。
- 接口上互不冲突：本文档不碰 `DoUpload` 内部，只改它的**调度与派发**；`03` 不碰队列/worker，只改 `DoUpload`。两者可独立合入、叠加生效。
- 数据结构需为 `03` 预留：`StageBlock` 可能新增"可选内存 buffer"字段（writeback 携带）。本设计的 Router/队列对 `StageBlock` 是按值搬运，新增字段无影响。

## 文件结构

```
src/cache/local/
├── block_cache_uploader.h     # 改：内部结构改为 per-disk；公开接口 EnterUploadQueue 不变
├── block_cache_uploader.cc    # 改：PendingQueue 加 cond_var；新增 DiskUploader 聚合；
│                              #     Router 路由；多 worker 启停
├── cache_store.h              # 改：CacheStore 新增 LocateDiskId(handle) 纯定位接口
├── disk_cache_group.cc/.h     # 改：实现 LocateDiskId = GetStore(handle)->Id()
├── disk_cache.h               # 改：实现 LocateDiskId = Id()
├── mem_cache.cc/.h            # 改：实现 LocateDiskId = 固定单 id
└── disk_cache_loader.cc       # 不改逻辑：仍调 uploader_(handle,len, {kFromReload, disk_id_})
                               #     —— store_id 已是路由权威，天然落到对应盘队列
src/cache/iutil/
└── inflight_tracker.h         # 不改：每盘各 new 一个实例即可
```

新增内部类型（均在 `block_cache_uploader.cc` 内，不污染头文件）：

```cpp
struct DiskUploader {                 // 一块盘的上传上下文
  std::string disk_id;
  PendingQueueUPtr queue;             // 含 cond_var
  iutil::InflightTrackerUPtr tracker; // 每盘限流 + 去重
  std::thread worker;                 // 该盘的 worker 线程
};
// BlockCacheUploader 持有:
//   std::unordered_map<std::string /*disk_id*/, std::unique_ptr<DiskUploader>> uploaders_;
//   bthread::Mutex uploaders_mutex_;  // 保护 map（盘集合变更时）
//   iutil::BthreadJoinerUPtr joiner_; // 全局共享，沿用
```

## 实现步骤

1. **给 `PendingQueue` 加事件驱动**（最小可验证步骤，可单独合入）
   - 增加 `bthread::ConditionVariable not_empty_`；`Push` 末尾 `notify_one`；新增 `PopBlocking(running)`；`Wake()` 供 shutdown 广播。
   - `UploadWorker` 改为 `PopBlocking`，删除 `sleep_for(10ms)`。
   - 此步即便仍是单队列单 worker，也已消除 10ms 轮询与 CPU 空转。先发布、先验证。

2. **抽出 `LocateDiskId` 定位接口**
   - `CacheStore` 增加 `virtual std::string LocateDiskId(const BlockHandle&) const = 0;`
   - `DiskCacheGroup`/`DiskCache`/`MemCache` 各自实现（见文件结构）。
   - 单测：对一批 handle，校验 `LocateDiskId(h)` 与 `GetStore(h)->Id()` 一致；reload 块走 `store_id`。

3. **引入 `DiskUploader` 与 Router**
   - `BlockCacheUploader` 内部由"单 `pending_queue_` + 单 `tracker_` + 单 `thread_`"改为 `uploaders_` map。
   - `Start()` 时按 `store_` 暴露的盘列表为每盘创建 `DiskUploader` 并起 worker；`MemCache` 退化为 1 个。
   - `EnterUploadQueue` 改为：`RouteDiskId(sblock)` → 找到/惰性创建对应 `DiskUploader` → `queue->Push`。
   - 新增参数 `FLAGS_upload_stage_max_inflights_per_disk`。

4. **多 worker 与 tracker 下沉**
   - `AsyncUpload`/`OnComplete` 改为以 `DiskUploader*` 为上下文：`tracker_` → `d->tracker`，重试 `EnterUploadQueue` 时按 `store_id` 仍路由回同盘。
   - `joiner_` 保持全局共享。

5. **生命周期与盘集合变更**
   - Shutdown：`running_=false` → 对每盘 `queue->Wake()` 广播 → join 所有 worker → `joiner_->Shutdown()`。
   - 与 `DiskCacheWatcher`（`disk_cache_watcher.{h,cc}`）协同：盘热插拔/重启时，新盘首个块到达即惰性创建 `DiskUploader`（见兼容性章节）。

6. **可选：MPSC 无锁化（方案 B）**
   - 仅当 profiling 显示单盘锁仍是热点时实施，用"两条 `bthread::ExecutionQueue`（wb/reload）"表达优先级。

7. **清理与文档**
   - 移除旧全局队列、`FLAGS_upload_stage_max_inflights` 的全局语义（或保留为"全局封顶"开关）。

## 兼容性与灰度

- **公开接口零变更**：`BlockCacheUploader::EnterUploadQueue`、`Start`、`Shutdown` 签名不变；`LocalBlockCache`/`DiskCacheLoader` 的调用点（`local_block_cache.cc:116`、`disk_cache_loader.cc:191`）**无需改动**——reload 块已携带 `store_id`，writeback 块路由规则与 `DiskCacheGroup` 完全一致。
- **语义等价**：优先级（wb > reload）、去重、限流、失败重试 100ms 后重入队，全部保留；只是限流粒度从"全局 32"变为"每盘 M"。
- **灰度开关**：新增 `FLAGS_enable_parallel_upload`（默认先 false）。为 true 时走 per-disk 多 worker；为 false 时退回单队列单 worker（但仍可享受步骤 1 的事件驱动）。线上先单盘开、再全量。
- **参数迁移**：
  - 旧 `--upload_stage_max_inflights=32` 默认行为：开启并行后映射为 `--upload_stage_max_inflights_per_disk=32`，并保持 `--upload_stage_max_inflights=0`（全局不封顶）。
  - 多盘部署下总在飞数上限从 32 提升为 `N × M`，需在 release note 中说明，避免突然打满后端连接池——必要时把 per-disk 默认调小（如 16）。
- **盘热插拔**：`DiskCacheWatcher` Restart 单盘时（`disk_cache_watcher.h:43`），该盘 worker 随 `DiskUploader` 惰性创建；移除盘时其 worker 在 Shutdown 阶段统一回收。一致性哈希变更后，writeback 块由 `LocateDiskId` 重新定位，仍落到正确盘；历史 reload 块靠 `store_id` 不受影响。
- **MemCache 路径**：行为与现状完全一致（单队列单 worker）。

## 风险与对策

| 风险 | 说明 | 对策 |
| --- | --- | --- |
| 路由不一致导致"队列在 A、数据在 B" | `LocateDiskId` 与 `DiskCacheGroup::GetStore` 规则若不同步会错盘 | 二者复用同一实现（`GetStore(handle)->Id()`）；单测对拍；reload 始终用 `store_id` 权威路由 |
| 总在飞数放大打爆后端 | per-disk M × N 盘可能远超原全局 32 | per-disk 默认调小（16）；提供可选全局封顶 tracker；灰度逐步放量 |
| 去重失效 | 同一块被两条 worker 同时上传 | 路由确定性保证同块永远同盘同 worker；去重由该盘 tracker 覆盖，正确性不依赖全局集合 |
| 条件变量唤醒丢失 / shutdown 卡死 | worker 在 `wait` 时错过 notify，或 shutdown 未唤醒 | `wait` 谓词同时检查 `running_`；Push 持锁内 notify；Shutdown 先置 `running_=false` 再对每盘 `notify_all` 广播 |
| 多 worker 线程数膨胀 | N 盘 = N 条 pthread worker | worker 只做"出队 + 派发"，极轻量；上传仍在 bthread 池；可改用常驻 bthread 而非 pthread 进一步降成本 |
| `tracker_->Add` 阻塞队头（残留） | 单盘内 worker 仍在 `Add` 处可能阻塞 | 这是预期的盘内背压；跨盘已隔离，互不影响。若需进一步解耦，可把 `Add` 的等待移入派发 bthread（代价是失去对派发速率的反压） |
| 与 03 合入顺序耦合 | 两改动并行开发 | 接口解耦（本文档不碰 `DoUpload`），任意顺序合入；`StageBlock` 新增字段按值搬运无碍 |
| 盘热插拔竞态 | 惰性创建 `DiskUploader` 与 Shutdown 并发 | `uploaders_mutex_` 保护 map；创建/查找在锁内；Shutdown 置位后不再新建 |

## 测试方案

### 单元测试（`tests/...`，Boost.Test）

1. **路由一致性**：构造多盘 `DiskCacheGroup`，对随机 handle 批量校验 `LocateDiskId(h) == GetStore(h)->Id()`；reload `StageBlock`（带 `store_id`）路由到对应盘队列。
2. **优先级保留**：向同一盘队列混入 wb/reload 块，断言 worker 先弹出全部 wb 再弹 reload（复用现有 `Segments`/`Pop` 优先级测试，迁移到 per-disk）。
3. **事件驱动正确性**：
   - 空队列时 worker 阻塞（无忙等，可用 CPU 计数或 hook 校验未走 sleep 分支）。
   - `Push` 后 worker 被及时唤醒并派发（断言延迟 << 10ms 量级，验证轮询已消除）。
   - Shutdown 时阻塞 worker 被广播唤醒并干净退出，无死锁、无泄漏。
4. **去重**：同一 key 连续 `EnterUploadQueue` 两次，断言只产生一次 `DoUpload`（mock 存储计数）。
5. **限流**：per-disk M 设为小值（如 2），并发投递多块，断言任一时刻同盘在飞数 ≤ M；释放后被等待者推进。
6. **失败重试**：mock `storage_client->Put` 返回可重试错误，断言 100ms 后重新入队并最终成功；CacheDown/NotFound 不重试。
7. **MemCache 退化**：`cache_store=memory` 时单队列单 worker，行为与现状一致。

### 集成 / 压测

8. **多盘吞吐对比**：N=4 盘、writeback 高频小块，对比并行前后总上传吞吐与单块上传起步延迟（P50/P99），验证吞吐随盘数提升、延迟抖动消除。
9. **重启 reload 恢复时间**：预置每盘数万残留 stage 文件，重启后测全部上传完成耗时，对比单 worker 基线。
10. **背压隔离**：人为让 A 盘对应存储后端变慢（注入延迟），断言 B 盘上传不受影响（验证队头阻塞已按盘隔离）。
11. **灰度回退**：`--enable_parallel_upload=false` 行为与历史版本一致（回归保护）。

### Sanitizer

12. 在 `sanitize` 构建（ASan/TSan）下跑上述 1-7，重点暴露多 worker + 条件变量 + 盘热插拔的数据竞争与死锁。

---

> 关联文档：`03`（消除上传时的本地盘回读）。本文档与 `03` 正交，建议先合入步骤 1（事件驱动）取得即时收益，再推进 per-disk 多 worker，最后视 profiling 决定是否启用 MPSC 无锁队列（方案 B）。
