# Per-core / 多 io_uring ring 改造（Phase 1）

## 概述

本文描述 DingoFS 本地磁盘缓存（`src/cache/local`）的 AIO 子系统改造方案，目标是消除当前"每盘单 ring + 单提交线程 + 单完成线程 + condvar 跨线程唤醒"的结构性瓶颈。

当前每块磁盘只有 **1 个 `IOUring`**、**1 个提交 pthread**（bthread ExecutionQueue）、**1 个完成 pthread**（`bg_wait_thread_`）。所有业务 bthread 把 `Aio` 漏斗式塞进同一条 ExecutionQueue，由唯一的提交线程串行 `prep + submit`；I/O 完成后由唯一的完成线程通过 `bthread::ConditionVariable` 逐个唤醒挂起的业务 bthread。这条单线程漏斗在高 iodepth（默认 `iodepth=128`）、多核高并发场景下成为吞吐和延迟的天花板。

Phase 1 的范围**不引入完整的 thread-per-core / 独立事件循环**（那是 Phase 2，见 [`06-per-core-event-loop.md`](./06-per-core-event-loop.md)），而是在现有 brpc bthread 执行模型下做"近似 per-core"：

- 把单 ring 拆成 **N 个 ring**（每个提交线程 / 每核 hint 一个 ring，外加注册 buffer 切片）；
- 让**提交（submit）与收割（reap）尽量发生在同一线程**，去掉提交线程 → 完成线程的二次交接；
- **减少 / 消除 `bthread ↔ pthread` 的 condvar handoff**，完成回调直接在收割线程上 `butex_wake` 唤醒业务 bthread。

保留现有的关键能力：**SQPOLL、注册 fixed buffer、O_DIRECT 对齐、`InflightTracker` 限流**。

---

## 背景与动机

### 证据：单 ring + 双线程漏斗

`src/cache/local/aio_queue.cc`：

- `AioQueue::AioQueue`（`aio_queue.cc:41-45`）：每个 `AioQueue` 只构造 **1 个 `IOUring`**。
- `AioQueue::Start`（`aio_queue.cc:61-65`）：启动 **1 条 `bthread::ExecutionQueue`**（`use_pthread = true`），所有提交都走这一条队列、由唯一的工作线程串行执行 `PrepareIO`。
- `AioQueue::Start`（`aio_queue.cc:67`）：启动 **1 个 `bg_wait_thread_`** 专门 `BackgroundWait` 收割完成事件。
- `AioQueue::PrepareIO`（`aio_queue.cc:98-126`）：以 `kSubmitBatchSize = 16`（`aio_queue.h:46`）为粒度批量 `prep` 后 `submit`。
- `AioQueue::BackgroundWait`（`aio_queue.cc:138-153`）：完成线程死循环 `WaitIO(1000ms)`，对每个完成的 `Aio` 调 `OnComplete → RunClosure → aio->Run()`。

`src/cache/local/io_uring.cc`：

- `InitIOUring`（`io_uring.cc:114-123`）：`use_sqpoll` 默认 `true`（`io_uring.h:42`），开 `IORING_SETUP_SQPOLL`。
- `RegisterBuffers`（`io_uring.cc:125-137`）：注册全部 fixed buffer（读写两个 slab pool 拼出的 `iovec` 数组）。
- `SetupEpoll`（`io_uring.cc:139-155`）：把 `io_uring_.ring_fd` 挂到一个 epoll 上。
- `WaitIO`（`io_uring.cc:217-240`）：`epoll_wait` 在 `ring_fd` 上等可读，再 `io_uring_for_each_cqe` 收割整批 CQE。

`src/cache/local/aio.h`：

- `Aio::Wait`（`aio.h:66-71`）：业务 bthread 提交后 `bthread::Mutex` + `ConditionVariable` 挂起。
- `Aio::Run`（`aio.h:73-77`）：完成线程加锁、置 `finish_ = true`、`notify_one()` 唤醒。这是典型的**跨线程交接**（提交在业务 bthread / 提交线程，唤醒在完成线程）。

`src/cache/local/local_filesystem.cc`：

- `AioWrite` / `AioRead`（`local_filesystem.cc:313-331`）：构造栈上 `Aio` → `aio_queue_->Submit(&aio)` → `aio.Wait()` 同步阻塞等待结果。
- `InflightAioGuard` + `inflight_(FLAGS_iodepth)`（`local_filesystem.cc:113-123, 129`）：以 `iodepth=128` 做在途限流。
- `O_DIRECT` + 4K 对齐（`local_filesystem.cc:200, 270, 333-347`，`kAlignedIOBlockSize = 4096`，`local_filesystem.h:70`）。

### 实例拓扑：每盘一套

- `DiskCache` 持有 1 个 `LocalFileSystem`（`disk_cache.cc:64`）。
- `LocalFileSystem` 持有 1 个 `AioQueue`（`local_filesystem.cc:130`）。
- `DiskCacheGroup` 管理 N 块盘（`disk_cache_group.cc:57`，`for i` 逐盘 `make_shared<DiskCache>`）。

即：**N 块盘 → N 套 `{1 ring + 1 提交线程 + 1 完成线程}`**。单盘内部仍是单 ring 单提交单完成。

### 问题归纳

1. **提交漏斗**：单盘所有业务 bthread 竞争同一条 ExecutionQueue，提交完全串行；该 ExecutionQueue 工作线程在单核上跑满即封顶单盘提交速率。
2. **二次线程交接**：提交线程 `prep+submit`，完成线程 `reap`。提交侧和完成侧不在同一线程，CQE 收割线程与 SQE 提交线程之间没有数据局部性，且需要额外一个常驻 pthread。
3. **condvar 跨线程唤醒**：完成线程逐个 `aio->Run()` 抢 `bthread::Mutex` 并 `notify_one()`，每个 I/O 都付出一次跨线程 mutex + 条件变量唤醒成本；高 IOPS 下 mutex 争用与唤醒延迟显著。
4. **epoll 间接等待**：`WaitIO` 经 `epoll_wait(ring_fd)` 再收割，多一层系统调用与唤醒链路。

---

## 当前实现分析

### 数据流（现状）

```
业务 bthread A ─┐
业务 bthread B ─┤  Submit(aio)            单条 ExecutionQueue
业务 bthread C ─┼───────────────────────► (use_pthread, 1 个工作线程)
   ...          │   execution_queue_execute        │
业务 bthread Z ─┘                                   │ PrepareIO: prep + 每 16 个 submit
                                                    ▼
                                          ┌───────────────────┐
                                          │   单个 IOUring     │  SQPOLL + fixed buf
                                          │   (ring_fd)        │
                                          └─────────┬─────────┘
                                                    │ CQE ready (epoll)
                                                    ▼
                                          bg_wait_thread_ (1 个 pthread)
                                              WaitIO → OnComplete
                                                    │ aio->Run():
                                                    │  lock(bthread::Mutex)
                                                    │  finish_=true; notify_one()
                                                    ▼
                            ┌───────────────────────┴──────────────────────┐
                       唤醒 bthread A          唤醒 bthread B   ...   唤醒 bthread Z
                       (Wait() 返回)           (Wait() 返回)         (Wait() 返回)
```

两处串行点 + 一处跨线程唤醒：

- **串行点 1**：单 ExecutionQueue 提交。
- **串行点 2**：单 `bg_wait_thread_` 收割。
- **跨线程唤醒**：`bg_wait_thread_` → 业务 bthread 的 `Mutex`/`ConditionVariable`。

### 为什么"提交 / 完成同线程"在当前模型下只能近似

brpc 的 bthread 是 M:N 用户态协程，调度在一组 pthread worker 上，业务 bthread **会迁移 worker**，没有稳定的"当前核"。因此 Phase 1 无法做到真正的"业务逻辑与 I/O 收割同核同线程"。我们采用的近似是：

- 用**固定数量的 ring + 固定的提交/收割线程**（pthread，不迁移），把"提交 + 收割"绑定在**同一个 worker 线程**上；
- 业务 bthread 仅负责 **把 `Aio` 路由到某个 ring**（无锁 / 轻量入队）并挂起等待；
- 真正彻底的"业务也跑在 per-core reactor 上、零 handoff"留给 Phase 2。

---

## 设计方案

### 总体思路

把 `AioQueue` 从"1 ring + 1 提交线程 + 1 完成线程"改造为 **`AioRing` 分片数组**：每个分片自带 `{IOUring, 提交+收割线程, fixed buffer 视图}`，业务 bthread 通过一个**路由函数**把 `Aio` 投递到某个分片；该分片的工作线程在**同一个 loop 内完成 prep / submit / reap / wake**，不再有提交线程→完成线程的二次交接。

分片数 `N = aio_ring_shards`（FLAG，默认取 `min(逻辑核数, 上限)`，可配）。这就是"per-core hint"：每核一个 ring，但因 bthread 会迁移 worker，故称"近似 per-core"。

### 数据结构

#### 1. `AioRing`（新增，单个分片）

每个 `AioRing` 拥有自己独立的 `io_uring`、提交+收割线程、以及一段 MPSC 投递队列。

```cpp
// src/cache/local/aio_ring.h（新增）
class AioRing {
 public:
  struct Options {
    uint32_t entries{4096};
    bool use_sqpoll{true};
    std::vector<iovec> fixed_buffers;  // 全量 fixed buffer（仍按全局 buf_index）
    int cpu_hint{-1};                   // >=0 时尝试 setaffinity（best-effort）
  };

  explicit AioRing(Options options);
  Status Start();
  Status Shutdown();

  // 业务 bthread 调用：把 aio 入队，唤醒本分片 loop。无锁 MPSC。
  void Enqueue(Aio* aio);

 private:
  void Loop();                          // 提交 + 收割同线程主循环
  int DrainPending(Aio* batch[], int max);  // 从 MPSC 队列取待提交 aio
  void PrepAndSubmit(Aio* batch[], int n);  // io_uring prep + submit
  int Reap(Aio* completed[]);               // io_uring_for_each_cqe 收割
  void Wake(Aio* aio);                      // 直接唤醒业务 bthread（butex）

  Options options_;
  std::atomic<bool> running_;
  io_uring io_uring_;
  int epoll_fd_{-1};                    // ring_fd + eventfd 两个源
  int wake_efd_{-1};                    // Enqueue 用它打断 loop 的等待
  bthread::ExecutionQueueId<Aio*> ingress_;  // 见“投递与唤醒”一节
  std::thread loop_thread_;             // 同时负责 submit 与 reap
};
```

要点：

- **submit 与 reap 在 `Loop()` 同一线程**：消除现状的提交线程 / 完成线程二次交接。
- **`wake_efd_`**：`Enqueue` 写 eventfd 打断 `epoll_wait`，让 loop 立刻去 `DrainPending`，避免依赖超时轮询。epoll 同时监听 `ring_fd`（CQE 就绪）和 `wake_efd_`（新提交到达）。
- **SQPOLL 保留**：开 SQPOLL 时内核侧轮询 SQ，`io_uring_submit` 仍需调用以推进，但减少了 `io_uring_enter` 系统调用频率；与多 ring 不冲突。
- **fixed buffer 仍全量注册**：`buf_index` 是相对"读写两个 slab pool 拼出的全局 iovec 数组"的全局下标（`FixedBuffers::GetIndex`，`local_filesystem.cc:94-99`）。**每个 ring 各自 `io_uring_register_buffers` 整份数组**，从而任意 `Aio` 落到任意 ring 都能用同一个 `buf_index` 命中 `*_fixed` 调用。代价是每 ring 一份内核侧 iovec 注册元数据（buffer 内存本体共享，不复制）。

#### 2. `AioQueue`（改造，分片容器 + 路由）

```cpp
// src/cache/local/aio_queue.h（改造）
class AioQueue {
 public:
  explicit AioQueue(const std::vector<iovec>& fixed_buffers);
  Status Start();
  Status Shutdown();
  void Submit(Aio* aio);                // 路由到某个 AioRing

 private:
  size_t RouteOf(const Aio* aio) const; // 选分片：见“路由策略”

  std::atomic<bool> running_;
  std::vector<std::unique_ptr<AioRing>> rings_;  // N 个分片
  std::atomic<uint64_t> rr_{0};         // round-robin 计数（回退策略用）
};
```

`Submit` 不再 `execution_queue_execute` 到全局单队列，而是 `rings_[RouteOf(aio)]->Enqueue(aio)`。

#### 3. `Aio`（改造唤醒，去 condvar）

保持 `Wait()/Run()` 外部语义不变，但内部把 `bthread::Mutex + ConditionVariable` 换成更轻的 **butex / `bthread::CountdownEvent`(初值 1)**，避免每个 I/O 的 mutex 争用：

```cpp
// src/cache/local/aio.h（改造）
void Wait() { done_.wait(); }            // butex_wait：让出 bthread，不阻塞 pthread
void Run()  { done_.signal(); }          // butex_wake：可从 reap 线程安全调用
// private: bthread::CountdownEvent done_{1};
```

`CountdownEvent` 内部是 butex，`signal()` 可从任意线程（含 ring 的 reap 线程）安全调用，唤醒在业务 bthread 所在 worker 上恢复。相比现状的"锁 + 条件变量"，每个 I/O 少一把 mutex 与一次 `notify_one` 的谓词检查。

> 注意：现状 `Aio` 是**栈上对象**（`AioWrite`/`AioRead` 里 `auto aio = Aio(...)`），`done_` 随栈存活；`Wait()` 返回前 reap 线程已 `signal()`，生命周期安全不变。

### 路由策略（`RouteOf`）

目标：同一文件的 I/O 尽量落到同一 ring（cache 局部性 + 顺序性），同时整体均衡。

- 默认按 `fd` 散列：`RouteOf(aio) = hash(aio->Attr().fd) % N`。`fd` 在 DingoFS 缓存里对应一个临时/块文件，按 fd 分片可保证同一文件的 prep/submit/reap 在固定 ring/线程上，利于内核侧文件级局部性，并天然契合 `InflightTracker` 以 `fd` 为 key 的限流（`local_filesystem.cc:119`）。
- 回退：当某 ring 持续积压（pending 深度超阈值）时，对**写**可退化为 round-robin（`rr_++`）以泄压；**读**保持按 fd（读放大依赖对齐 buffer 复用）。
- `N` 由 `--aio_ring_shards` 控制，默认 `clamp(sysconf(_SC_NPROCESSORS_ONLN), 1, kMaxRingShards)`；单核或显式设 1 时退化为"接近现状但已合并提交/完成线程"的形态。

### 投递与唤醒（消除二次交接）

业务 bthread → ring 的投递必须**无阻塞、可跨线程安全唤醒 loop**。两种实现，二选一：

- **方案 A（推荐，复用 brpc）**：每个 `AioRing` 持有一条 `bthread::ExecutionQueue<Aio*>`（`use_pthread=true`），其 consumer 即 ring 的 loop。`Enqueue = execution_queue_execute`。consumer 回调里做 `prep+submit`，loop 自身再做 `reap+wake`。注意：此处仍有"提交在 ExecutionQueue 工作线程、reap 在 loop 线程"的风险——**因此 consumer 与 reap 必须是同一个线程**：让 ExecutionQueue 不真正搬运执行，而仅作为"有新任务"的信号 + MPSC 容器，loop 线程在被唤醒后统一 `prep+submit+reap`。
- **方案 B（自管 MPSC + eventfd）**：无锁 MPSC intrusive 链表（`Aio` 加一个 `next_` 指针）+ `wake_efd_`。`Enqueue` 原子 push 后写 eventfd；loop 的 epoll 同时等 `ring_fd` 与 `wake_efd_`，醒来后 `DrainPending` 一次性取走整条链批量 `prep+submit`，再 `reap` 已完成 CQE，最后逐个 `Wake`。**这是真正"提交+收割同线程"的形态**，去掉了对 brpc ExecutionQueue 工作线程的依赖。

本设计以**方案 B** 为主线（彻底同线程），方案 A 作为最小改动的过渡选项。无论哪种，关键不变量是：**一个分片的 prep / submit / reap / wake 全部在该分片唯一的 loop 线程上完成**，业务 bthread 只做入队 + butex 等待。

### 改造后数据流（ASCII）

```
                            AioQueue::Submit(aio)  →  RouteOf(aio) = hash(fd) % N
                                          │
        ┌─────────────────────────────────┼─────────────────────────────────┐
        ▼                                 ▼                                  ▼
   ┌─────────┐                       ┌─────────┐                        ┌─────────┐
   │ AioRing0│                       │ AioRing1│        ...             │AioRing N-1│
   │         │ Enqueue(MPSC+efd)     │         │                        │         │
   │  loop 线程（提交+收割同线程）    │  loop 线程                       │  loop 线程│
   │  ┌────────────────────────────┐ │  ...                            │  ...     │
   │  │ epoll_wait(ring_fd, efd)   │ │                                 │          │
   │  │   ├─ efd 醒: DrainPending  │ │                                 │          │
   │  │   │     → prep + submit    │ │                                 │          │
   │  │   └─ ring_fd 醒: reap CQE  │ │                                 │          │
   │  │         → Wake(aio): butex │ │                                 │          │
   │  └────────────────────────────┘ │                                 │          │
   │  io_uring0  SQPOLL+fixed buf    │  io_uring1  SQPOLL+fixed buf    │ io_uringN-1│
   └─────────┘                       └─────────┘                        └─────────┘
        │                                 │                                  │
        └──────── butex_wake ─────────────┴────────── butex_wake ───────────┘
                 直接唤醒发起该 aio 的业务 bthread（Wait() 返回）
```

对比现状，去掉了：单提交 ExecutionQueue 漏斗（拆成 N 个）、单 `bg_wait_thread_`（与提交合并进 loop）、`Mutex`/`ConditionVariable` 逐 I/O 唤醒（换 butex）。

### 保留的能力

- **SQPOLL**：每个 ring 各自开 `IORING_SETUP_SQPOLL`（沿用 `io_uring.cc:115`）。多核多 ring 时可考虑 `IORING_SETUP_SQ_AFF` 把 SQ poll 线程绑核 + `IORING_SETUP_ATTACH_WQ` 共享内核 worker 池，避免 N 个 SQPOLL 内核线程吃满 CPU（见“风险与对策”）。
- **注册 fixed buffer**：每个 ring 全量注册（`io_uring.cc:125-137` 逻辑下沉到 `AioRing`），`buf_index` 全局语义不变（`FixedBuffers::IsFixed/GetIndex`）。
- **O_DIRECT + 4K 对齐**：完全在 `LocalFileSystem` 侧，改造不触碰（`local_filesystem.cc:200,270,335-347`）。
- **`InflightTracker` 限流**：仍在 `AioWrite/AioRead` 入口（`local_filesystem.cc:315,325`），`iodepth=128` 语义不变；分片只影响"在途 I/O 如何分布到 ring"，不影响总在途上限。

---

## 文件结构

```
src/cache/local/
├── aio.h                 # 改造：Wait/Run 改用 bthread::CountdownEvent（butex），去 Mutex/ConditionVariable
├── aio_ring.h            # 新增：单分片 AioRing 声明（io_uring + loop 线程 + MPSC/efd）
├── aio_ring.cc           # 新增：AioRing 实现（prep/submit/reap/wake 同线程 loop）
├── aio_queue.h           # 改造：持有 vector<AioRing>，新增 RouteOf 路由
├── aio_queue.cc          # 改造：Submit 路由到分片；Start/Shutdown 管理 N 个 ring
├── io_uring.h            # 收敛：把 ring 生命周期 + prep/submit/reap 原语下沉给 AioRing 复用
├── io_uring.cc           # 收敛/复用：PrepRead/PrepWrite/RegisterBuffers/OnComplete 复用，去掉自带 epoll 完成等待的单 ring 假设
└── local_filesystem.cc   # 基本不变（AioWrite/AioRead 仍 Submit + Wait）

common/options/ 或对应 FLAGS 定义处
└── 新增 FLAGS：
    --aio_ring_shards (uint32, default 0=auto)   # 分片数 / ring 数
    --aio_ring_sqpoll (bool,   default true)     # 与现 use_sqpoll 对齐
    --aio_ring_sq_affinity (bool, default false) # 是否给 SQPOLL 线程绑核
    --aio_route_policy (string, default "fd")    # fd | rr
```

> 复用策略：`IOUring` 已有的 `PrepRead/PrepWrite/OnComplete/RegisterBuffers/SetupEpoll` 逻辑高度可复用，建议将其作为 `AioRing` 的内部成员或基类，不重写 io_uring 原语，仅改"由谁驱动 loop、submit 与 reap 是否同线程"。

---

## 实现步骤

1. **抽出 `AioRing`（不改行为）**
   - 把 `IOUring` 的 ring 生命周期 + `PrepRead/PrepWrite/SubmitIO/WaitIO/OnComplete` 包成 `AioRing`，先做成"1 个 ring + loop 线程（提交+收割合一）"，路由临时固定到 ring 0。
   - 此步已经合并了提交线程与完成线程：`Loop()` 内 `DrainPending → prep → submit → reap → wake`。验证 `local_filesystem` 单测、`aio` 相关单测全绿。

2. **`Aio` 唤醒去 condvar**
   - `aio.h` 的 `Wait/Run` 改 `bthread::CountdownEvent done_{1}`。
   - 单测覆盖：提交→完成→`Wait` 返回；错误路径（`OnError` 也走 `Run`，`aio_queue.cc:155-159`）。

3. **MPSC + eventfd 投递**
   - `Aio` 加 intrusive `next_` 指针；实现无锁 MPSC `Enqueue/DrainPending`。
   - `AioRing` 的 epoll 同时监听 `ring_fd` 与 `wake_efd_`；`Enqueue` 写 efd 唤醒。
   - 压测确认：批量到达时一次 `DrainPending` 批量 `prep+submit`，避免逐个 `io_uring_enter`。

4. **分片化 `AioQueue`**
   - `AioQueue` 持 `vector<AioRing>`，`Start/Shutdown` 起停全部 ring。
   - 每个 ring 全量 `RegisterBuffers`（沿用现有 `FixedBuffers::Fetch()` 的全量 iovec，`local_filesystem.cc:130`）。
   - 实现 `RouteOf`（默认 `hash(fd)%N`）。

5. **FLAGS 与绑核**
   - 加 `--aio_ring_shards`（0=auto=`min(NPROC, 上限)`）、`--aio_route_policy`、`--aio_ring_sq_affinity`。
   - 可选：`cpu_hint>=0` 时对 loop 线程 `pthread_setaffinity_np`（best-effort，失败仅告警）。

6. **SQPOLL 多 ring 优化（可选，按压测结果开启）**
   - 评估 `IORING_SETUP_ATTACH_WQ` 让多 ring 共享内核 worker；`IORING_SETUP_SQ_AFF` 绑定 SQ poll 核。

7. **指标与可观测性**
   - 每 ring 暴露：pending 深度、submit 批均值、reap 批均值、`io_uring_enter` 次数、唤醒延迟分位。
   - 复用 `DiskCacheGroupMetrics` 风格（`disk_cache_group.cc:41`）接入 bvar。

---

## 兼容性与灰度

- **外部接口零变更**：`LocalFileSystem::AioWrite/AioRead`、`AioQueue::Submit`、`Aio::Wait` 签名与语义不变；上层 `DiskCache` / `DiskCacheGroup` 无感。
- **退化为现状**：`--aio_ring_shards=1` 即单分片，行为最接近现状（仅多了"提交+完成合一 + butex"两项优化），可作为对照基线。
- **灰度开关**：保留旧 `AioQueue` 实现（编译期或 `--aio_impl=legacy|sharded` 运行期开关），先在部分缓存盘 / 测试集群启用 `sharded`，对比 P99 与 CPU 后全量。
- **内核/特性回退**：`AioRing::Start` 中 `Supported()`（沿用 `io_uring.cc:102-112`）失败、或 `register_buffers` 失败时，按盘回退到 `shards=1` 且不使用 fixed buffer（`buf_index=-1` 路径已存在，走非 `_fixed` 调用，`io_uring.cc:170-188`）。
- **配置缺省安全**：`--aio_ring_shards=0` 在容器 cgroup 限核场景下应读 `sched_getaffinity` 可用核数而非物理核数，避免起过多 ring。

---

## 风险与对策

| 风险 | 说明 | 对策 |
| --- | --- | --- |
| SQPOLL 内核线程膨胀 | N 个 ring × 每 ring 1 个 SQPOLL 内核线程，多盘 × 多 ring 可能吃满 CPU | 默认 `shards=min(NPROC,上限)` 且按盘共享；用 `IORING_SETUP_ATTACH_WQ` 共享 worker；高并发不足时再开 SQPOLL，否则可关 SQPOLL 用普通 submit |
| fixed buffer 重复注册 | 每 ring 全量注册一份 iovec，注册元数据 ×N | buffer 本体共享（仅注册描述符），内存开销小；若内核侧上限受限，可改为"按 ring 切分 buffer 区段 + 路由保证 buf 与 ring 对齐"（复杂度更高，留作优化） |
| 路由倾斜 | `hash(fd)%N` 在热点文件下倾斜，单 ring 过载 | pending 阈值触发写 round-robin 泄压；监控每 ring pending 分位 |
| bthread 迁移导致唤醒跨核 | 业务 bthread 在哪个 worker 恢复不可控，butex_wake 后仍可能跨核 | Phase 1 接受此近似；彻底解决见 Phase 2 reactor（业务也跑在 per-core loop） |
| Aio 栈对象生命周期 | reap 线程 `Wake` 与业务 bthread `Wait` 竞争栈对象 | `CountdownEvent`(butex) 保证 `Wait` 在 `signal` 后才返回，栈对象在 `Wait` 返回前不析构，与现状同步语义一致 |
| MPSC 实现正确性 | 无锁 MPSC + eventfd 边界条件（丢唤醒/ABA） | 优先选方案 A（复用 brpc ExecutionQueue 做 MPSC + 唤醒），仅在压测确认收益后切方案 B；MPSC 配合 efd 时采用"先入队后写 efd、loop 先 drain 后清 efd"的标准序避免丢唤醒 |
| 关停顺序 | N 个 loop 线程 + ring 资源释放顺序 | `Shutdown`：停止接受新 Enqueue → 逐 ring drain 完在途 → join loop → `unregister_buffers` → `queue_exit`，复用现 `Cleanup`（`io_uring.cc:157-168`） |

---

## 测试方案

### 单元测试

- `aio_test` / `io_uring_test`（沿用现有 `FRIEND_TEST(IOUringTest, FixedBuffers/OnComplete)`，`io_uring.h:59-60`）：
  - `AioRing` 单 ring：提交 N 个读/写，全部完成且 `OnComplete` 结果正确（含 short-io、负 res 错误路径，`io_uring.cc:242-257`）。
  - `Aio::Wait/Run` 改 butex 后：正常完成、错误完成（`OnError` 路径）、并发多 `Aio` 不丢唤醒。
  - fixed buffer：`buf_index>=0` 走 `*_fixed`、`buf_index=-1` 走普通调用，两路都正确（`io_uring.cc:170-188`）。
- `aio_queue_test`：
  - `RouteOf` 分布：同 fd 稳定落同 ring；不同 fd 大致均衡。
  - `shards=1` 与 `shards=N` 行为一致性（结果正确性）。
  - 关停：在途 I/O 全部 drain、无泄漏（ASan/`sanitize` 构建）。

### 集成 / 文件系统测试

- `local_filesystem_test`：`WriteFile/ReadFile` 端到端，含非对齐 offset/length（`local_filesystem.cc:278-301`）、fixed 与非 fixed buffer 两路、`O_DIRECT` 对齐校验。
- `InflightTracker` 限流：iodepth 打满时仍正确背压、不死锁（`inflight_tracker.h` 的 `Add` 会等待）。

### 性能测试

- 复用 `tests/perf` 框架，新增/扩展 AIO 基准：
  - 维度：`shards ∈ {1, 2, 4, 8}` × `iodepth ∈ {32,128,512}` × 读/写 × 块大小（4K/64K/1M）。
  - 指标：IOPS、吞吐、提交侧 CPU、reap 侧 CPU、submit/reap 批均值、`io_uring_enter` 次数、P50/P99/P999 延迟。
  - 对照：`legacy`（现状单 ring 双线程）vs `sharded shards=1`（合并线程+butex）vs `sharded shards=N`。
  - 期望：`shards=1` 已因去 condvar / 合并线程降低 P99 与 CPU；`shards=N` 在多核多并发下提升 IOPS 上限、提交不再单核封顶。
- 构建/运行（沿用项目约定）：
  ```bash
  ninja -C build/release tests/perf/<aio_bench_target>
  ./test.py --mode release --name aio
  # sanitize 验证内存/并发安全
  ./test.py --mode sanitize --name aio_ring
  ```

### 稳定性 / 故障注入

- 磁盘不健康（`DiskHealthChecker`，`local_filesystem.cc:179,256`）下提交被拒，ring 不残留在途。
- short io / EIO 注入：`OnComplete` 标错并经 `Wake` 正确返回上层。
- 反复 `Start/Shutdown` 无 fd/线程/buffer 泄漏（`sanitize` + `lsof` 抽检）。
