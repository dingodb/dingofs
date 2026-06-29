# 绑核 + 每核事件循环运行时骨架（取消 Cache/Prefetch 每请求起新 bthread）

## 概述

本文档（Phase 2 / 06）描述 dingo-cache 缓存节点数据面的**运行时骨架重构**：从当前「进程级 NUMA 软绑定 + 每请求 `bthread_start_background`」的模型，演进为「**每核 pin 一个 worker 线程、各跑一个事件循环（thread-per-core）**」的模型。

核心目标：

1. **绑核（CPU pinning）**：在现有进程级 NUMA 绑定之上，为数据面 worker 线程逐个绑定到固定逻辑核，消除跨核迁移、cache line 跨核失效、NUMA 远端访存。
2. **每核事件循环（per-core event loop）**：每个绑核 worker 线程承载一个独立的事件循环（运行时载体推荐用 **Photon vcpu**，承接 08 协程化文档的结论），其上跑协程而非 OS 线程。
3. **取消 Cache/Prefetch 每请求起 bthread**：把 `LocalBlockCache::AsyncCache` / `AsyncPrefetch`（`src/cache/local/local_block_cache.cc`）从「每请求 `iutil::RunInBthread`」改为「按 key 哈希**投递到本核循环的任务队列**」，请求在所属核的事件循环里以协程跑完。

本文档只交付**运行时骨架**（绑核、每核 loop、handoff 队列、Cache/Prefetch 投递改造），与以下两篇协同但不重复：

- **05（key→核分派）**：定义「`block_id` / `handle.Id()` → 核」的分派函数与一致性，本文档复用其分派结果做投递目标选择。
- **08（协程化 / Photon）**：定义 I/O 路径协程化（`pread`/网络改 Photon 异步原语）。本文档只搭载体，不改 I/O 语义；08 落地后每核 loop 内的同步阻塞调用会被替换为协程让出。

---

## 背景与动机

### 1. 当前只有进程级 NUMA 软绑定，无 per-core pinning

`src/cache/node/dingo_cache.cc:135` 在 `Run()` 中调用：

```cpp
// src/cache/node/dingo_cache.cc:135
utils::BindNumaOrDie();
```

`BindNumaOrDie()`（`src/utils/numa_binder.cc:41-78`）的全部能力是：

- `numa_run_on_node(node)`（`numa_binder.cc:56`）——把**整个进程**的调度约束到某个 NUMA node 的 CPU 集合；
- `numa_set_preferred / numa_set_membind / numa_set_interleave_mask`（`numa_binder.cc:60-71`）——设内存分配策略。

这是 **node 级软绑定**：进程的所有线程仍可在该 node 内的任意核之间被内核自由迁移。全仓 grep 验证数据面**无 per-core 绑定**：

```
$ grep -rn 'affinity|CPU_SET|pthread_setaffinity|sched_setaffinity' src/
src/common/blockaccess/bench/block_access_bench.cc:41   CPU_SET(...)   // 仅 bench 工具
src/utils/concurrent/task_thread_pool.h:71              CPU_SET(...)   // 通用工具，数据面未使用
```

`task_thread_pool.h:68-78` 已提供 `BindThreadToCpu(uint32_t cpu_id)`（`pthread_setaffinity_np`），但 dingo-cache 数据面**从未调用**它。结果：

- bthread worker、brpc I/O 线程在 node 内随机漂移；
- 同一请求的不同阶段可能落在不同核，L1/L2 反复 miss；
- 与「per-core 数据结构 / per-disk 队列」无法对齐。

### 2. Cache / Prefetch 每请求起一个新 bthread

`LocalBlockCache::AsyncCache`（`src/cache/local/local_block_cache.cc:256-282`）与 `AsyncPrefetch`（`:284-310`）的核心都是「每请求一个 `iutil::RunInBthread`」：

```cpp
// src/cache/local/local_block_cache.cc:270 (AsyncCache 摘录)
auto tid = iutil::RunInBthread(
    [tracker, self, handle, block = std::move(block), cb, option]() mutable {
      Status status = self->Cache(handle, std::move(block), option);
      if (cb) cb(status);
      tracker->Remove(handle.Filename());
    });
if (tid != 0) {
  joiner_->BackgroundJoin(tid);   // 投递到 BthreadJoiner 的 execution_queue 回收
}
```

`iutil::RunInBthread`（`src/cache/iutil/bthread.cc:42-66`）= `bthread_start_background`；回收由 `BthreadJoiner`（`bthread.cc:68-120`）的 `bthread::execution_queue` 串行 `bthread_join`。

问题：

- **每请求一次 bthread 创建 + 一次入队 join**：高 QPS 下是稳定的栈分配 / 调度 / 回收开销；
- **调度位置不可控**：新 bthread 由 bthread 调度器随机派发到任意 worker pthread，与「该 key 归属的核」无关，破坏数据局部性；
- **回收串行化点**：所有 `Async*` 的 tid 都汇聚到单个 `BthreadJoiner` execution_queue（`local_block_cache.cc:95` 仅一个 joiner），形成全局串行回收瓶颈；
- `AsyncPut` / `AsyncRange`（`:217-254`）同样每请求起 bthread，本轮先聚焦 Cache/Prefetch（写回填充 / 预取，纯异步、无外部等待方），是改造性价比最高的入口。

> 同样模式也存在于 `src/cache/remote/remote_block_cache.cc:159-216` 与 `src/cache/local/block_cache_uploader.cc:225`；本文档骨架可复用，但落地范围仅 local 的 Cache/Prefetch。

### 3. brpc handler 在 bthread worker 上**同步**执行

`BlockCacheServiceImpl`（`src/cache/node/service.cc`）的每个 handler 都在 brpc 派发的 bthread worker 上**同步**跑业务：

- `Range`（`service.cc:121-140`）：同步 `node_->Range(...)`，内部 `RetrieveCache` / `RetrieveStorage`（`node.cc:194-211`）是阻塞磁盘 / 网络读；
- `Cache`（`service.cc:142-157`）/ `Prefetch`（`:159-170`）：handler 内仅做 `AsyncCache` / `AsyncPrefetch`（立即返回 OK，真正工作甩给上面的 bthread）。

这说明：**控制/请求入口在 brpc（bthread）上是既成事实且必须保留**（brpc 负责 RDMA/TCP 协议栈、attachment 零拷贝 `GetRequestAttachment`/`SetResponseAttachment`，见 `service.cc:80-105`）。重构必须在 **brpc 边界处**把数据面工作 handoff 进每核事件循环，而不是推翻 brpc。

### 动机小结

| 维度 | 现状 | 目标 |
|------|------|------|
| 绑核 | node 级软绑定 | 每核硬 pin worker |
| 并发载体 | 每请求 1 bthread | 每核 1 loop，N 协程复用 |
| 调度局部性 | 随机派发 | key→核固定派发 |
| 回收 | 全局 1 个 joiner 串行 join | 无 join（协程跑完即回收） |

---

## 当前实现分析

### 数据面调用链（现状）

```
brpc/RDMA recv
   └─ bthread worker (随机核)
        └─ BlockCacheServiceImpl::Cache / Prefetch        (service.cc:142 / :159)
             └─ CacheNode::AsyncCache / AsyncPrefetch       (node.cc:213 / :231)
                  └─ LocalBlockCache::AsyncCache/AsyncPrefetch  (local_block_cache.cc:256 / :284)
                       └─ iutil::RunInBthread  →  新 bthread (又一个随机核)
                            └─ LocalBlockCache::Cache/Prefetch  (同步磁盘+网络 I/O)
                       └─ joiner_->BackgroundJoin(tid)  →  单一 execution_queue 串行回收
```

涉及对象（`LocalBlockCache` 成员，`local_block_cache.cc:89-97`）：

- `store_`：`DiskCacheGroup`，内部用 `KetamaConHash` 按 `handle.Id()` 把 block 分派到某块盘（`disk_cache_group.cc:183-190` `GetStore`）。**已经存在一套「key→盘」的一致性哈希**，是 05「key→核」分派的天然锚点。
- `joiner_`：单一 `BthreadJoiner`。
- `cache_tracker_` / `prefetch_tracker_`：`InflightTracker`（`iutil/inflight_tracker.h`），用 `bthread::Mutex`/`condition_variable` 做并发去重与背压（上限 1024）。**这把锁在每核单线程模型下可降级/去除**（见设计方案）。

### 关键观察

1. `DiskCacheGroup::GetStore(handle)` 已用 `chash_->Lookup(handle.Id(), node)` 把同一 block 稳定映射到同一块盘——**盘是天然的分片单元**，每核绑一组盘即可让「key→核」与「key→盘」自洽（见「核数与盘/网卡映射」）。
2. `InflightTracker` 的去重锁、`BthreadJoiner` 的回收队列，都是「多线程随机访问共享态」的产物。一旦改为「同一 key 永远落同一核、同核单线程串行」，这些跨核同步可大幅简化。
3. brpc handler 与业务之间已经是「`AsyncCache` 立即返回 + 后台执行」的解耦形态，**handoff 语义已存在**，只需把 handoff 目标从「随机 bthread」换成「本核任务队列」。

---

## 设计方案

### 总览：控制面留 brpc，数据面进每核事件循环

```
                          ┌───────────────────────── dingo-cache 进程 ─────────────────────────┐
                          │                                                                     │
   client ──RDMA/TCP──▶   │  ┌── brpc / RDMA 控制面（bthread worker pool，不绑核）──┐            │
                          │  │  BlockCacheServiceImpl::Range/Cache/Prefetch          │            │
                          │  │  - 协议解析 / attachment 零拷贝                        │            │
                          │  │  - JoinGroup / Heartbeat / Ping / metrics（控制面）   │            │
                          │  └───────────────┬──────────────────────────────────────┘            │
                          │                  │  handoff: key=handle.Id() → core = Dispatch(key)   │
                          │                  ▼  (05 定义 Dispatch；MPSC 队列 + eventfd 唤醒)       │
                          │   ┌──────────────────────── CoreRuntime ───────────────────────────┐ │
                          │   │  core 0 (pinned)   core 1 (pinned)   ...   core N-1 (pinned)    │ │
                          │   │  ┌────────────┐    ┌────────────┐         ┌────────────┐        │ │
                          │   │  │ EventLoop  │    │ EventLoop  │   ...   │ EventLoop  │        │ │
                          │   │  │ (Photon    │    │ (Photon    │         │ (Photon    │        │ │
                          │   │  │  vcpu)     │    │  vcpu)     │         │  vcpu)     │        │ │
                          │   │  │ TaskQueue  │    │ TaskQueue  │         │ TaskQueue  │        │ │
                          │   │  │  └─coro─┐  │    │  └─coro─┐  │         │  └─coro─┐  │        │ │
                          │   │  │  Cache/  │  │    │  Cache/  │  │       │  Cache/  │  │        │ │
                          │   │  │  Prefetch│  │    │  Prefetch│  │       │  Prefetch│  │        │ │
                          │   └──┴─────┬────┴──┴────┴─────┬────┴──┴───────┴─────┬────┴──┴────────┘ │
                          │           │ 本核绑定的盘组      │                    │                  │
                          │     disk[0..k)            disk[k..2k)          disk[..]                │
                          └─────────────────────────────────────────────────────────────────────┘
```

边界定义：

| 平面 | 载体 | 内容 | 绑核 |
|------|------|------|------|
| **控制面** | brpc + bthread（现状不变） | RPC 收发、协议解析、attachment、心跳/JoinGroup/Ping、reload flags、bvar metrics | 否（保持 brpc 默认线程池） |
| **数据面** | CoreRuntime：N 个绑核 worker，每核 1 个事件循环 | Cache 填充、Prefetch 拉取、（后续）Range 读 | 是（逐核 `pthread_setaffinity_np`） |
| **桥接** | per-core handoff 队列（MPSC） | brpc bthread → 目标核 loop 的任务投递 + eventfd 唤醒 | 跨边界唯一同步点 |

### 1. 运行时载体：Photon vcpu（而非自建 loop）

承接 08 文档的推荐，**每核事件循环采用 Photon 的 vcpu + 协程**，理由：

- Photon 的运行模型天然就是「**1 OS 线程 = 1 vcpu = 1 事件循环**」，`photon::init()` + 每核一个 `photon::thread_migrate` / per-thread `init` 即对应 thread-per-core；
- 自带 io_uring / epoll engine、协程原语（`photon::thread`）、定时器、信号量、`WorkPool`，无需自研调度器；
- 与 08 的「I/O 协程化」目标同源：08 落地后 `pread` / 网络读直接换成 Photon 的异步原语，在同一 vcpu 内让出而不阻塞 worker，承载体不变；
- 跨核投递可用 Photon 的线程间唤醒（基于 eventfd 的 `Photon::thread_interrupt` / `WorkPool::async_call`），与我们的 handoff 队列语义一致。

**对比自建 loop**：自建（epoll + 自研协程 / 状态机）灵活但要重造调度、定时、I/O 引擎、跨核唤醒，工作量与风险都高于直接用 Photon。结论：**用 Photon vcpu**。

> 为隔离风险，CoreRuntime 暴露一个**最小内部接口** `CoreLoop`（见文件结构），Photon 作为其默认实现；保留「自建 epoll loop」作为兜底实现（接口同构），灰度期可一键切换。

### 2. 核数与盘 / 网卡的映射

设进程被分到 NUMA node `node`（沿用 `--numa_node`），该 node 内可用逻辑核集合为 `C = {c0, c1, ...}`（由 `numa_node_to_cpus` 读取，与 `BindNumaOrDie` 同源）。

- **worker 核数** `W = min(len(C) - reserved, FLAGS_data_plane_cores)`：
  - `reserved`：为 brpc bthread worker / RDMA poller / 日志 / 心跳预留的核（默认 ≥ 2），避免数据面把控制面饿死；
  - `FLAGS_data_plane_cores`：可显式指定（默认 0 = 自动按上式）。
- **worker → 核**：worker `i` 绑到 `C[reserved + i]`（调用 `utils::BindThreadToCpu`，`task_thread_pool.h:68`）。
- **核 → 盘**：复用 `DiskCacheGroup` 现有「`handle.Id()` → 盘」一致性哈希。新增映射 `disk → core`：把 `M` 块盘**轮转或按 NUMA 亲和**分给 `W` 个核，使「`handle.Id()` → 盘 → 核」与 05 的「`handle.Id()` → 核」**取同一个 Dispatch 结果**（关键一致性约束，见下）。
  - 常见部署 `M ≈ W`（每核 1~2 块 NVMe）：核与盘 1:1 / 1:2，I/O 队列天然 per-core，无锁竞争。
  - 盘多于核：一个核负责多块盘的 loop（盘是被动资源，无需独占核）。
- **核 → 网卡**：RDMA / TCP 收发仍在 brpc/RDMA poller（控制面）；数据面 worker 不直接持网卡。若后续做 per-core RX queue（RSS / SO_REUSEPORT），可让 worker `i` 亲和到与其同 NUMA 的网卡队列——本期不做，留作 05/网络专项。

**一致性约束（与 05 对齐）**：

```
Dispatch(handle.Id())  ==  DiskToCore[ GetStore(handle.Id()).index ]
```

即「05 决定的归属核」必须与「该 key 所在盘的归属核」一致，否则会出现「在 A 核的 loop 里读 B 核绑定的盘」，破坏 per-core 无锁假设。落地上：**先确定盘→核映射 `DiskToCore`，再令 05 的 `Dispatch` 直接定义为 `DiskToCore[chash.Lookup(id).index]`**，从根上保证两者同源（05 文档据此定稿）。

### 3. handoff 队列：brpc bthread → 目标核 loop

每核一个 **MPSC 队列**（多生产者：任意 brpc bthread；单消费者：该核 loop）：

- 入队：`CoreRuntime::Post(core, Task)` —— 无锁 MPSC（如 `boost::lockfree::queue` 或 bthread 的 `ExecutionQueue` use_pthread 变体；优先选无锁 ring + eventfd）；
- 唤醒：写 eventfd / Photon 线程间中断，loop 从 I/O 等待中醒来取任务；
- 任务：`std::function<void()>`（或更轻的 POD `{op, handle, IOBuffer, cb}`，避免 `std::function` 堆分配，见风险章）。

**handoff 是跨边界唯一同步点**：队列入队是无锁原子操作，eventfd 唤醒是一次 `write(8B)`，远轻于 `bthread_start_background` + `execution_queue` join。

### 4. Cache / Prefetch 改造：投递到本核 loop，取消起 bthread

改 `LocalBlockCache::AsyncCache` / `AsyncPrefetch`：

```cpp
// 改造后（示意）—— local_block_cache.cc:256 AsyncCache
void LocalBlockCache::AsyncCache(BlockHandle handle, IOBuffer block,
                                 AsyncCallback cb, CacheOption option) {
  DCHECK_RUNNING("LocalBlockCache");

  const int core = runtime_->Dispatch(handle.Id());   // 05 分派；与盘归属同源
  runtime_->Post(core,
      [this, handle = std::move(handle), block = std::move(block),
       cb = std::move(cb), option]() mutable {
        // 在 core 的事件循环里、以 Photon 协程执行；
        // 同核串行 → InflightTracker 去重退化为本核哈希集合查重，无需 bthread::Mutex
        Status status = this->Cache(handle, std::move(block), option);
        if (cb) cb(status);
      });
}
```

变化点：

1. **删除** `iutil::RunInBthread` + `joiner_->BackgroundJoin(tid)`（`local_block_cache.cc:270-281` / `:298-309`）。
2. **去重 / 背压下沉到核内**：`cache_tracker_` / `prefetch_tracker_`（`InflightTracker`）从「全局 `bthread::Mutex` 集合」改为**每核一个普通 `unordered_set` + 计数**（单线程访问，无锁）。背压由「核内 inflight 上限 + 队列水位」表达；队列满时对 Cache/Prefetch（best-effort）直接丢弃并计数（与现有 `IsCached` 短路、`Status::Exist` 行为兼容）。
3. **`BthreadJoiner` 在 Cache/Prefetch 路径不再需要**；若 `AsyncPut`/`AsyncRange` 本期仍走旧路则 joiner 保留，否则可整体下线（分阶段，见实现步骤）。
4. `CacheNode::AsyncCache`/`AsyncPrefetch`（`node.cc:213-243`）签名不变，内部回调语义不变——上层无感。

### 5. 阻塞 I/O 的过渡形态（与 08 衔接）

- **08 之前（骨架期）**：loop 内的 `this->Cache` / `this->Prefetch` 仍是同步阻塞（`pread` / `storage_client->Range`）。此时「每核 1 loop」意味着**该核同一时刻只有 1 个 Cache/Prefetch 在跑**，需用 **Photon vcpu 内的多协程 + WorkPool** 把阻塞 I/O 放到该核的 I/O 协程，避免单协程阻塞憋住整核（即：投递的任务 `photon::thread_create` 成一个协程，磁盘/网络读用 Photon 文件/socket 异步原语让出）。
- **08 之后**：`this->Cache`/`this->Prefetch` 内部 I/O 全面协程化，loop 内并发度由协程数决定，吞吐随之提升。本文档保证**承载体到位**，08 只换 I/O 原语。

---

## 文件结构

```
src/cache/
├── runtime/                         # 新增：每核事件循环运行时骨架
│   ├── core_runtime.h               # CoreRuntime：管理 N 个绑核 CoreLoop，提供 Dispatch / Post
│   ├── core_runtime.cc
│   ├── core_loop.h                  # CoreLoop 接口（Start/Stop/Post/RunOnLoop）
│   ├── photon_core_loop.h           # 默认实现：Photon vcpu + 协程
│   ├── photon_core_loop.cc
│   ├── epoll_core_loop.h            # 兜底实现：自建 epoll loop（灰度对照，可选）
│   ├── epoll_core_loop.cc
│   ├── handoff_queue.h              # per-core MPSC 队列 + eventfd 唤醒
│   ├── cpu_topology.h               # NUMA node → cpu 列表、disk→core 映射计算
│   ├── cpu_topology.cc
│   └── CMakeLists.txt
├── local/
│   └── local_block_cache.cc         # 改：AsyncCache/AsyncPrefetch 改投递；去 RunInBthread/joiner
├── node/
│   ├── dingo_cache.cc               # 改：BindNumaOrDie 之后初始化并启动 CoreRuntime
│   └── cache_server.cc              # 改：Start/Shutdown 串联 CoreRuntime 生命周期
└── iutil/
    └── inflight_tracker.h           # 改/新增：per-core 无锁版本（或保留旧版供控制面）

src/utils/
└── concurrent/task_thread_pool.h    # 复用 BindThreadToCpu / GetCpuCount（已存在，:63-78）
```

新增最小公共接口（`core_loop.h` 示意）：

```cpp
namespace dingofs::cache::runtime {

class CoreLoop {  // Photon / epoll 两种实现
 public:
  virtual ~CoreLoop() = default;
  virtual Status Start(int cpu_id) = 0;     // 绑核 + 起 loop（含 Photon vcpu init）
  virtual void Shutdown() = 0;
  virtual void Post(Task task) = 0;         // 跨线程投递（MPSC 入队 + 唤醒）
};

class CoreRuntime {
 public:
  Status Start();                           // 读拓扑，建 W 个 CoreLoop 并绑核
  void Shutdown();
  int  Dispatch(uint64_t key) const;        // 05：key→核（= DiskToCore[chash(key)]）
  void Post(int core, Task task);           // 转发到 cores_[core]->Post
 private:
  std::vector<std::unique_ptr<CoreLoop>> cores_;
  std::vector<int> disk_to_core_;           // 盘→核，保证与 chash 同源
};

}  // namespace
```

---

## 实现步骤

分阶段、可独立验证、可回滚：

**Step 0 — CPU 拓扑与绑核工具（无行为变化）**
- 新增 `runtime/cpu_topology.{h,cc}`：基于 libnuma `numa_node_to_cpus` 列出 `--numa_node` 内的逻辑核；计算 `W`、`reserved`、`disk_to_core_`。
- 复用 `utils::BindThreadToCpu`（`task_thread_pool.h:68`）。
- 单测：给定 node/核数/盘数，断言映射稳定、覆盖、与 `KetamaConHash` 同源。

**Step 1 — CoreRuntime + CoreLoop（Photon 实现），默认关闭**
- 新增 `core_runtime`、`photon_core_loop`、`handoff_queue`。
- 每核 `pthread` 起 Photon vcpu（`photon::init` per-thread engine = io_uring 优先，epoll 兜底），绑核后进入 loop，消费 handoff 队列。
- 加 flag `--enable_per_core_runtime`（默认 `false`）。
- `CacheServer::Start`/`Shutdown`（`cache_server.cc:62`/`:109`）串生命周期；`dingo_cache.cc:135` `BindNumaOrDie()` 之后、`GlobalInitOrDie()` 之前/之后初始化 runtime。
- 单测：投递 K 个空任务，断言每个都在「目标核」线程上执行（用 `sched_getcpu()` 校验绑核）。

**Step 2 — Cache/Prefetch 双路接入（flag 切换）**
- `LocalBlockCache` 持有 `CoreRuntime*`。`AsyncCache`/`AsyncPrefetch`（`local_block_cache.cc:256`/`:284`）：
  - `--enable_per_core_runtime=true` → `runtime_->Post(Dispatch(id), task)`；
  - `false` → 旧 `RunInBthread` 路径（保留）。
- 去重：新增 per-core `InflightTracker`（无锁版），仅在新路启用。
- 集成测试：两条路结果一致（落盘内容、回调 status、metrics）。

**Step 3 — 核内 I/O 协程化对接点（与 08 协同）**
- loop 内把每个投递任务 `photon::thread_create` 成协程；当前同步 I/O 包一层「Photon WorkPool 异步执行」防止单协程憋整核。
- 08 落地后替换为真正的 Photon 异步 I/O 原语。

**Step 4 — 收口与清理**
- 灰度稳定后默认 `--enable_per_core_runtime=true`。
- Cache/Prefetch 路径删除 `iutil::RunInBthread` 调用与对应 `joiner_` 依赖（`local_block_cache.cc:95/123/137`）；若 `AsyncPut`/`AsyncRange` 也迁移，则整体下线 `BthreadJoiner`。
- 文档化 flag、拓扑探测、回退方式。

---

## 兼容性与灰度

- **flag 总开关**：`--enable_per_core_runtime`（默认 `false`）。关闭时行为与现状 100% 一致（旧 bthread 路径原样保留），零回归风险。
- **配置**：
  - `--data_plane_cores`（默认 0 = 自动）：数据面 worker 核数；
  - `--data_plane_reserved_cores`（默认 2）：给控制面预留；
  - `--per_core_queue_depth`（默认如 4096）：handoff 队列深度（背压 / 丢弃阈值）；
  - `--per_core_loop_impl`（`photon` | `epoll`，默认 `photon`）。
- **NUMA 共存**：本特性叠加在 `--numa_node` 之上，不修改 `BindNumaOrDie`；`--numa_node=-1`（未绑 NUMA）时退化为「在全机核上分配 worker」，仍可绑核。
- **API 不变**：`CacheNode` / `BlockCacheServiceImpl` / `block_cache.h` 公共接口与回调语义不变；客户端无感。
- **灰度**：先在单节点开 flag 对照 metrics（QPS、p99、cache-fill 延迟、CPU migration 次数），再逐节点放量；任一异常即关 flag 秒回退。

---

## 风险与对策

| 风险 | 说明 | 对策 |
|------|------|------|
| **单协程阻塞憋整核** | 骨架期 I/O 仍同步，一个 Cache/Prefetch 阻塞会让整核 loop 停摆 | Step 3：每任务起 Photon 协程 + WorkPool 跑阻塞 I/O；08 前限制每核 inflight，监控 loop 滞留时长 |
| **分派与盘归属不一致** | 05 的 `Dispatch` 若与 `chash(handle.Id())→盘` 不同源，会跨核访盘 | 强约束 `Dispatch := DiskToCore[chash.Lookup(id)]`；单测断言一致性；05 据此定稿 |
| **热点核倾斜** | 某些 `handle.Id()` 集中 → 单核过载 | 监控 per-core 队列水位 / 利用率；倾斜时调整 `disk_to_core_` 或在 05 引入再哈希；队列满即 best-effort 丢弃（Cache/Prefetch 可重来） |
| **控制面被饿死** | 数据面占满核，brpc/RDMA poller / 心跳缺核 | `reserved_cores` 预留 ≥ 2；控制面不绑核，靠内核在剩余核调度；心跳/JoinGroup 留 brpc |
| **背压语义变化** | `InflightTracker`（1024，`bthread::cond` 阻塞）→ 队列水位丢弃 | 保持「`IsExist` 短路 / `Status::Exist`」对外语义；新增丢弃计数 bvar；对 best-effort 的 Cache/Prefetch 丢弃可接受 |
| **`std::function` 堆分配抵消收益** | 每次 Post 一个 `std::function` 仍有堆分配 | 任务用定长 POD（op + handle + IOBuffer move + cb），或对象池；IOBuffer 本就 move 语义零拷贝 |
| **Photon 与 brpc/bthread 共存** | 同进程内 Photon vcpu 与 bthread 调度器并存，TLS / 信号 / 栈冲突 | Photon 仅跑在绑核 worker 线程（独立 pthread），不与 bthread worker 混用线程；信号统一交 brpc（`InstallSignal`，`cache_server.cc:131`）；先在隔离线程验证 `photon::init` 与 bthread 不互踩 |
| **NUMA 拓扑探测失败** | 容器 / cgroup 限核下 `numa_node_to_cpus` 与实际可用核不符 | 探测失败或核数不足时 `--enable_per_core_runtime` 自动降级为关闭并告警，回退旧路径 |
| **优雅退出** | loop 内有 inflight 协程时 Shutdown | `Shutdown` 先停 handoff 入队、drain 队列、等核内 inflight 归零、再停 vcpu；与 `CacheServer::Shutdown`（`cache_server.cc:109`）顺序串联 |

---

## 测试方案

**单元测试**

1. `cpu_topology_test`：给定 `--numa_node` / 核数 / 盘数，断言 `W`、`reserved`、`disk_to_core_` 稳定且覆盖；断言 `Dispatch(id) == DiskToCore[KetamaConHash.Lookup(id)]`（与 `disk_cache_group.cc:183` 同源）。
2. `core_loop_test`：`Post` K 个任务，每个任务内 `sched_getcpu()` 必须等于其目标核（验证绑核生效）；并发多生产者投递无丢任务、无数据竞争（ASan/TSan）。
3. `handoff_queue_test`：MPSC 正确性、eventfd 唤醒、队列满时丢弃计数正确。
4. `inflight_tracker_percore_test`：无锁版去重 / 计数 / 背压与旧版语义等价（单线程）。

**集成测试**

5. **双路一致性**：同一组 Cache/Prefetch 请求，`--enable_per_core_runtime` 开 / 关两次运行，落盘内容、回调 status、`dingofs_disk_cache_group_*` metrics 一致。
6. **优雅退出**：制造 inflight 时 `Shutdown`，无 use-after-free（ASan）、无任务丢失、无 hang。
7. **NUMA 降级**：限核环境（`taskset` / cgroup）下探测失败能自动关 flag 回退。

**性能测试**（用 `src/cache/bench/` 现有 benchmarker，`benchmarker.cc:134`）

8. 固定 QPS 压 Cache/Prefetch，对照开 / 关 flag：
   - 关注 p50/p99 延迟、吞吐、CPU 利用率；
   - **CPU migration**：`perf stat -e migrations`（或 `/proc/<pid>/sched`）验证 worker 不再跨核漂移；
   - **缓存命中**：`perf stat -e cache-misses,LLC-load-misses` 对比；
   - bthread 创建计数应在新路下归零（去 `RunInBthread`）。
9. 多盘（M>W / M=W / M<W）三种映射下吞吐与单盘队列深度对照，验证 per-core 队列无锁化收益。

**回归**

10. 全量 `./test.py --mode dev`（cache 相关）确认 flag 关闭时零回归。
