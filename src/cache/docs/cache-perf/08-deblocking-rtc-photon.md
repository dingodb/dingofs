# 缓存数据路径全面去阻塞 / Run-to-Completion 化（Phase 2，RTC 核心）

## 概述

本文档描述 DingoFS Cache（远端缓存节点 / 本地盘缓存）数据路径的第二阶段性能改造：**全面去阻塞、Run-to-Completion（RTC）化**。

当前数据路径上每一次"提交异步任务 → 阻塞执行体 → 等他线程唤醒"的同步等待，都会让承载请求的执行体（bthread）挂起、等待跨线程的条件变量唤醒，再被调度器重新调度回来。这带来三类开销：

1. **上下文切换与唤醒延迟**：每个阻塞点都要一次 `futex/butex` 唤醒 + 一次重新调度，多个阻塞点串起来后（RDMA 入口 → 限流 → singleflight → 回源 GET / 本地 AIO）放大为多次跨核唤醒。
2. **跨核缓存抖动**：提交者、I/O 完成者、唤醒后续跑者往往不在同一个核，请求上下文（IOBuffer、Controller、Block 元数据）在核间来回搬运。
3. **执行体占用**：等待中的 bthread 虽然让出了 pthread，但其栈、TLS、关联的请求对象始终驻留，inflight 一多就放大内存与调度压力。

改造目标：把上述阻塞等待替换为 **continuation / `co_await`**，I/O 或子任务完成事件**在本核（提交它的那个核）续跑后续逻辑，而不挂起 / 唤醒执行体**——即 run-to-completion：一个请求从进入到完成，尽量在同一个核上、以"提交—回调续跑"的方式一气呵成，不做跨核的阻塞 handoff。

承接 Q900 选型结论：协程运行时采用 **Photon**（有栈协程 + io_uring 事件引擎），而非 Seastar。理由见下文"背景与动机"。

---

## 背景与动机

### 当前数据路径的阻塞点（file:line）

远端缓存节点处理一个 RDMA 请求的完整调用链上，串联了 5 处阻塞等待，全部基于 bthread 同步原语（`bthread::ConditionVariable` / `bthread::CountdownEvent`）：

1. **RDMA 服务入口同步等业务**
   `src/cache/infiniband/server_session.cc:241-256`
   ```cpp
   struct BlockingClosure : public ::google::protobuf::Closure {
     void Wait() { inflight.wait(); }            // :242
     void Run() override { inflight.signal(); }  // :243
     bthread::CountdownEvent inflight{1};        // :244
   };
   void ServerSession::ProcessRequest(...) {
     BlockingClosure done;
     service->CallMethod(method, controller, request, response, &done);  // :254
     done.Wait();                                                        // :255
   }
   ```
   每条 RDMA 完成（`OnNewMessage`，`server_session.cc:159`）用 `iutil::RunInBthread` 起一个 bthread 跑 `HandleNewMessage`，在该 bthread 上同步调用 brpc service 并 `done.Wait()` 等业务跑完。

2. **回源对象存储 GET / PUT 等待**
   `src/cache/common/storage_client.h:48`（`TaskClosure::Wait()`），调用点 `src/cache/common/storage_client.cc:269`（`Put`）、`:283`（`Range`）。
   `Range` 把任务压入 `use_pthread=true` 的 `bthread::ExecutionQueue`，在该 pthread 上调 `BlockAccesser::AsyncGet`，提交者 bthread `task.Wait()` 阻塞，直到异步回调 `OnComplete → TaskClosure::Run()` 唤醒。

3. **Singleflight 去重等待**
   `src/cache/common/task_tracker.h:67`（`DownloadTask::Wait(timeout_ms)`），调用点 `src/cache/node/node.cc:365`（`CacheNode::WaitTask`）。
   同一个 block 的并发回源，leader 跑 `RunTask`（内部又走阻塞点 2），follower `task->Wait(timeout)` 阻塞等 leader `task->Run()` 唤醒。

4. **限流（inflight 上限）等待**
   `src/cache/iutil/inflight_tracker.h:48`（`cond_.wait`），真正会"满了就等"的调用点是本地 AIO：`src/cache/local/local_filesystem.cc:116`（`InflightAioGuard`），上限 `FLAGS_iodepth`。
   ```cpp
   while (inflights_ + 1 > max_inflights_) {
     cond_.wait(lock);   // :48
   }
   ```

5. **本地盘 AIO（io_uring）完成等待**
   `src/cache/local/aio.h:66`（`Aio::Wait()`），调用点 `src/cache/local/local_filesystem.cc:319`（`AioWrite`）、`:329`（`AioRead`）。
   `Aio` 栈上构造，`AioQueue::Submit` 压入 `use_pthread=true` 的执行队列由 pthread 准备并 `io_uring_submit`，一条**专用 `std::thread`**（`aio_queue.cc:67` `bg_wait_thread_`）轮询 `WaitIO` 拿到完成后 `aio->Run()` 唤醒提交者 bthread。

### 现状的本质：bthread + 多套后台 pthread 的"阻塞—唤醒"拼接

注意现状已经做了一层规避：所有"会真正阻塞 OS 线程"的脏活（io_uring 提交/轮询、对象存储 SDK 的 pthread 同步语义）都被**显式甩到独立 pthread / std::thread / TaskThreadPool 上**（`aio_queue.cc:61-67`、`storage_client.cc:213-218`），bthread 只在 bthread CV 上等。这避免了"阻塞 bthread worker 的底层 pthread"这个致命问题，但代价正是上面列的多次跨核唤醒。

也就是说，**数据路径目前是"bthread 阻塞等待 + 后台 pthread 干活"的事件拼接，而不是 run-to-completion**。Phase 2 要做的，是把"后台 pthread 干活 + 唤醒 bthread"换成"在协程里 `co_await` 一个挂在本核 io_uring 上的 I/O，完成后在本核续跑"，从而消掉跨核唤醒与执行体挂起。

### 为什么是 Photon 而不是 Seastar（Q900 结论复述）

| 维度 | Seastar | Photon |
|------|---------|--------|
| 进程模型 | **独占 main**、share-nothing、自己接管 reactor 与 `malloc`（seastar allocator） | 库形态，**不接管 main、不接管 malloc**，可在任意 pthread 上 `init` 出一个 vcpu |
| 与 brpc/bthread 共存 | 几乎不可能：要求独占线程模型，与 bthread 调度器、brpc 的 TLS/线程假设冲突 | **可共存**：每核 1 个 pthread 跑 Photon vcpu，与 brpc worker pthread 并列；二者通过显式队列交互 |
| 与 RDMA / 现有 ibverbs 栈 | 需整体迁到 Seastar 的网络栈 | 不强制，RDMA 收发仍走现有 `cache/infiniband`，只把"等业务完成"协程化 |
| 协程类型 | stackless（continuation，需把同步代码重写成状态机 / `co_await` 链） | **有栈协程**，现有同步风格代码塞进协程几乎不改语义 |
| 改造工作量 | 大：数据路径要整体重写为 future/continuation | 小：阻塞 `Wait()` → `co_await` 一个 photon 信号 / I/O，业务逻辑顺序不变 |
| 存储领域生产验证 | 偏数据库（ScyllaDB） | **overlaybd / PolarFS 在用**，存储 I/O 场景生产验证 |

关键拦路点是 Seastar 要**独占 main 并接管 malloc**——DingoFS 缓存节点是 brpc 进程，bthread + brpc + RDMA(ibverbs) 已经牢牢占住进程主结构，Seastar 无法塞进来。Photon 是库，可以"每核一个 vcpu"嵌入到现有进程，因此选 Photon。

有栈协程的工作量优势对本次改造尤其关键：阻塞点 2/3/5 的业务体（`StorageClient::Range` 里的回源逻辑、`CacheNode::RetrieveWholeBlock` 的 singleflight、`LocalFileSystem::AioRead/Write`）都是**顺序同步代码**，迁到 Photon 协程后只需把那一行阻塞 `Wait()` 换成 `co_await`，函数体其余顺序逻辑（包括局部变量、RAII guard、错误分支）原样保留，几乎零语义改写。换成 Seastar 的 stackless future 则要把整段逻辑拆成 `.then()` 链或重写为 `co_await` 状态机，且每个捕获要小心生命周期。

---

## 当前阻塞点清单

| # | 阻塞点 | 定义 file:line | 调用点 file:line | 等待原语 | 等待者运行体 | 唤醒者 |
|---|--------|----------------|------------------|----------|--------------|--------|
| 1 | RDMA 入口同步等业务 | `server_session.cc:241-256` | 同上 `:255` | `bthread::CountdownEvent` | `RunInBthread` 起的 bthread（`server_session.cc:159`） | protobuf service `done->Run()` |
| 2a | 回源 PUT | `storage_client.h:48` | `storage_client.cc:269` | `bthread::ConditionVariable` | 提交者 bthread | `TaskThreadPool` worker（pthread） |
| 2b | 回源 GET（Range） | `storage_client.h:48` | `storage_client.cc:283` | `bthread::ConditionVariable` | 提交者 bthread | `ExecutionQueue`（`use_pthread=true`）pthread |
| 3 | singleflight 去重 | `task_tracker.h:67` | `node.cc:365` | `bthread::ConditionVariable`（`wait_for`） | follower bthread | leader bthread `task->Run()` |
| 4 | inflight 限流 | `inflight_tracker.h:48` | `local_filesystem.cc:116` | `bthread::ConditionVariable` | 提交者 bthread | `InflightAioGuard` 析构 `Remove` |
| 5 | 本地盘 AIO 完成 | `aio.h:66` | `local_filesystem.cc:319/329` | `bthread::ConditionVariable` | 提交者 bthread | 专用 `std::thread` 轮询 io_uring 后 `aio->Run()` |

> 串联关系：阻塞点 1 是请求入口，其内部业务（`CacheNode::Range`）会依次触发 3 → 2b（回源命中时）或 4 → 5（本地盘命中时），即**一次请求最多串 3 个阻塞点 + 3 次跨核唤醒**。

---

## 设计方案

### 总体思路：每核一个 Photon vcpu，数据路径协程化，bthread/Photon 显式 handoff

```
        现有（brpc 进程）                          Phase 2 引入
  ┌──────────────────────────┐          ┌──────────────────────────────┐
  │ pthread #i  (brpc worker) │          │ pthread #i  (brpc worker)     │
  │   └─ bthread schedulers   │   绑核    │   └─ bthread schedulers       │
  └──────────────────────────┘  pinning  ├──────────────────────────────┤
                                          │ pthread #i' (Photon vcpu #i)  │
                                          │   └─ photon::thread + io_uring│
                                          └──────────────────────────────┘
```

- **每核一个 Photon vcpu**：在每个 NUMA-绑定的 CPU 核上额外起一个 pthread，调用 `photon::init(INIT_EVENT_IO_URING, INIT_IO_NONE)` 初始化为一个 Photon vcpu，自带该核的 io_uring 事件引擎。数据路径的 I/O（本地盘读写、回源对象存储、本核内的 photon 信号）全部挂在**本核的 io_uring**上，完成事件由本核 vcpu 在 `photon::thread_yield` 处续跑，**不跨核**。
- **bthread / Photon 是两套独立调度器，互不阻塞**：绝对禁止在 Photon vcpu 上调用任何 bthread 阻塞原语（`bthread::Mutex/ConditionVariable/CountdownEvent`、`bthread_usleep`、brpc 同步 RPC），也禁止在 bthread 上调用 photon 阻塞原语——任何一边的阻塞调用都会卡死另一边的整个 vcpu。跨界一律走**显式 handoff 队列**（见下）。

### Photon 接入：vcpu 生命周期与绑核

新增 `src/cache/photon/` 模块，封装 vcpu 池：

```cpp
// src/cache/photon/photon_env.h（新增）
namespace dingofs::cache::photon {

// 每核一个 vcpu，绑定到与 brpc worker 相同的核（复用 utils/numa_binder）
class PhotonEnv {
 public:
  Status Start(int num_vcpus);   // 起 num_vcpus 个 pthread，各自 photon::init
  void Shutdown();

  // 把一个协程函数投递到第 i 个 vcpu 上执行（线程安全，可从 bthread 侧调用）
  void Submit(int vcpu_idx, std::function<void()> coro);
  int  CurrentVcpu() const;      // 若当前 pthread 是 photon vcpu，返回其 idx，否则 -1
 private:
  struct Vcpu { std::thread th; photon::vcpu_base* vcpu; /* mpsc 投递队列 */ };
  std::vector<Vcpu> vcpus_;
};

}  // namespace dingofs::cache::photon
```

每个 vcpu pthread 主循环：
```cpp
void VcpuMain(int idx) {
  bind_to_core(idx);                                   // 复用 src/utils/numa_binder
  photon::init(photon::INIT_EVENT_IO_URING, photon::INIT_IO_NONE);
  DEFER(photon::fini());
  while (running_) {
    // 从 mpsc 队列取出待执行协程，photon::thread_create 后 yield
    drain_submit_queue_and_spawn();
    photon::thread_yield();                            // 让 io_uring 引擎跑 I/O 完成回调
  }
}
```

### bthread ↔ Photon 边界桥接（显式 handoff，禁止跨界阻塞）

两个方向：

**方向 A（bthread → Photon）**：bthread 想发起一段需要协程化的 I/O。
通过 `PhotonEnv::Submit(vcpu, coro)` 把协程投递到目标 vcpu 的 mpsc 队列（无锁入队 + `eventfd`/`photon::semaphore` 唤醒 vcpu），bthread 侧**不阻塞等待结果**——而是把"后续处理"也作为 continuation 一并交给协程，请求上下文随协程在 Photon 侧 run-to-completion。仅在需要把结果交还 bthread 时（极少数，如要回到 brpc 的同步 API）才用一个 bthread 侧的 `butex` 等待，且这是受控的、非数据路径热点。

**方向 B（Photon → bthread/brpc）**：Photon 协程里要调用现有 brpc/bthread-only 的接口（例如还没协程化的元数据 RPC）。
绝不能在 Photon vcpu 上直接调 brpc 同步接口。改为把调用 handoff 给 brpc，用 brpc 的**异步回调**（`done` 回调），回调里通过 `PhotonEnv` 把"完成事件"投回原 vcpu 唤醒协程：

```cpp
// 在 photon 协程内调用一个 brpc 异步接口，并 co_await 其完成
photon::semaphore sem(0);
Result result;
SomeService_Stub stub(channel);
auto* done = brpc::NewCallback(+[](photon::semaphore* s, Result* r, Controller* c, Resp* resp){
  *r = FromResp(c, resp);
  s->signal(1);                       // 从 brpc 回调线程唤醒 photon 协程（跨线程安全）
}, &sem, &result, cntl, resp);
stub.Method(cntl, req, resp, done);   // 不阻塞 photon vcpu
sem.wait();                           // photon 协程让出，I/O 完成后本 vcpu 续跑
```

> 关键纪律：`photon::semaphore::signal` 是 vcpu-safe 的跨线程唤醒，brpc 回调（在某个 brpc pthread）调用它会把目标 vcpu 唤醒并把协程排回**目标 vcpu 的本核运行队列**，续跑仍在该核——保持 run-to-completion。

### 各阻塞点改造对照

下面给出每个阻塞点改造前/后的代码形态。核心模式统一：**`XxxClosure::Wait()`（bthread CV）→ `co_await`（photon 信号 / photon io_uring I/O）**，业务体顺序逻辑不变（有栈协程优势）。

#### 阻塞点 5：本地盘 AIO 完成（`Aio::Wait` → photon io_uring）

这是 RTC 化收益最大、也最干净的一处：现状的"`use_pthread` 执行队列准备 + 专用 std::thread 轮询 + bthread CV 唤醒"三段式，可直接坍缩成"在本核 vcpu 上 `co_await` 一次 photon 的 io_uring preadv/pwritev"。

改造前（`local_filesystem.cc:313-331` + `aio_queue.cc` + `aio.h`）：
```cpp
Status LocalFileSystem::AioRead(int fd, char* buffer, size_t length, int buf_index) {
  InflightAioGuard guard(fd, &inflight_);          // 阻塞点 4
  auto aio = Aio(fd, 0, length, buffer, buf_index, true);
  aio_queue_->Submit(&aio);                        // 压 use_pthread 队列
  aio.Wait();                                       // 阻塞点 5：bthread CV 等专用 std::thread 唤醒
  return aio.Result().status;
}
```

改造后（在 photon 协程内，本核 io_uring）：
```cpp
// 运行在某个 Photon vcpu 上
Status LocalFileSystem::AioReadCoro(int fd, char* buffer, size_t length, off_t offset) {
  // photon 自带 io_uring 引擎，preadv 直接 co_await（有栈：写法同同步代码）
  ssize_t n = photon::fs::preadv(fd, ...);   // 内部挂本核 io_uring，完成后本 vcpu 续跑
  return n == (ssize_t)length ? Status::OK() : Status::IoError("aio read");
}
```
收益：去掉 `bg_wait_thread_` 专用轮询线程、`prep_io_queue_id_` 执行队列、`Aio::Wait` 的跨线程唤醒；提交—完成—续跑全在本核，固定缓冲（`IOUringOptions::fixed_buffers`）改用 photon 的 fixed buffer / registered fd 能力保留零拷贝。

#### 阻塞点 4：inflight 限流（`cond_.wait` → photon::semaphore）

`InflightTracker` 的"满了就等"语义天然就是一个计数信号量。

改造前（`inflight_tracker.h:42-63`）：`bthread::Mutex` + `bthread::ConditionVariable`，`while (inflights+1 > max) cond_.wait`。
改造后：一个 `photon::semaphore sem(max_inflights)`，`Add` = `sem.wait(1)`（满则让出本协程，不阻塞 vcpu），`Remove` = `sem.signal(1)`；去重集合 `busy_` 用 photon 的 `mutex`（vcpu 内非抢占，多数情况无竞争）保护。注意：限流计数现在是**每 vcpu 局部**或用 vcpu-safe 信号量做全局，按 iodepth 语义选择（建议本核 io_uring 深度本就该 per-vcpu，故 per-vcpu 信号量更契合 RTC）。

#### 阻塞点 2：回源对象存储 GET/PUT（`TaskClosure::Wait` → co_await 异步 SDK）

改造前（`storage_client.cc:277-289`）：压 `use_pthread` 执行队列调 `AsyncGet`，`task.Wait()` 等 bthread CV。
改造后：在 photon 协程内直接调 `BlockAccesser::AsyncGet`（其本身就是异步回调接口），回调里 `photon::semaphore::signal` 唤醒本协程：
```cpp
Status StorageClient::RangeCoro(BlockHandle h, off_t off, size_t len, IOBuffer* out) {
  photon::semaphore sem(0);
  Status st;
  auto ctx = MakeGetCtx(h, off, len, out);
  ctx->cb = [&](Status s){ st = s; sem.signal(1); };   // SDK 回调线程 → 唤醒本 vcpu 协程
  block_accesser_->AsyncGet(ctx);                       // 不阻塞 vcpu
  sem.wait();                                            // 本协程让出，完成后本核续跑
  return st;
}
```
> 若底层对象存储 SDK 内部有 pthread 同步语义（`storage_client.h:113-116` 注释提到的风险），它发生在 SDK 自己的线程池里，不在 Photon vcpu 上；vcpu 只在 `sem.wait()` 让出，安全。这里不再需要专门的 `use_pthread` 执行队列做隔离——隔离由"SDK 异步回调 + photon 信号唤醒"天然完成。

#### 阻塞点 3：singleflight 去重（`DownloadTask::Wait` → photon condition/semaphore）

改造前（`task_tracker.h` + `node.cc:325-374`）：leader 跑 `RunTask`，follower `task->Wait(timeout)` 等 bthread CV，带超时。
改造后：`DownloadTask` 内换成 `photon::semaphore`（或 `photon::condition_variable`），`Wait` = `sem.wait(timeout)`（photon 原生支持带超时让出），`Run` = `sem.signal(N)` 唤醒所有 follower。`TaskTracker` 的 `tasks_` map 用 photon `mutex` 保护。leader/follower 同在 Photon 侧，唤醒不跨调度器。

#### 阻塞点 1：RDMA 入口同步等业务（`BlockingClosure` → 协程化请求处理）

改造前（`server_session.cc:150-256`）：RDMA 完成回调 `RunInBthread` 起 bthread 跑 `HandleNewMessage`，内部 `ProcessRequest` 用 `BlockingClosure`(`CountdownEvent`) 同步等 brpc service。
改造后：RDMA 完成回调不再起 bthread，而是 `PhotonEnv::Submit(本核 vcpu, HandleNewMessageCoro)` 把整条请求处理投到**收到它的那个核的 Photon vcpu**。`HandleNewMessageCoro` 在协程里顺序执行：解析 → 读 attachment（RDMA read，协程化）→ 调用业务（`CacheNode::Range` 已是 photon 协程，直接函数调用而非 brpc CallMethod）→ 发送响应。整条链 run-to-completion，无 `done.Wait()`。

> 对仍需经 brpc service 分发的路径（保留兼容期），用"方向 B"的 brpc 异步回调 + photon 信号桥接，避免 `CountdownEvent` 阻塞。

### 改造前后调用流对照图

**改造前（一次远端 Range 命中回源，跨 3 个阻塞点、≥3 次跨核唤醒）：**
```
RDMA poller ──OnNewMessage──> RunInBthread() ──> [bthread A] HandleNewMessage
   [bthread A] ProcessRequest: done.Wait() ◄─────────────── done->Run()    （阻塞点1，唤醒①）
       └─ CacheNode::Range ─ RetrieveWholeBlock
            ├─ follower: DownloadTask::Wait() ◄──── leader task->Run()      （阻塞点3，唤醒②）
            └─ leader: StorageClient::Range
                  push use_pthread EQ ──> [pthread P] AsyncGet ── SDK线程
                  task.Wait() ◄───────────────────── OnComplete/Run()       （阻塞点2，唤醒③）
   （若本地盘命中：）AioRead: guard(阻塞点4) + aio.Wait() ◄── bg std::thread （阻塞点5，唤醒④）
```
执行体在 bthread A、pthread P、SDK 线程、bg std::thread 之间来回，请求上下文跨核搬运。

**改造后（全部在收到请求的本核 vcpu，run-to-completion，无跨核阻塞唤醒）：**
```
RDMA poller ──OnNewMessage──> PhotonEnv::Submit(本核vcpu, HandleNewMessageCoro)
   [vcpu #i] HandleNewMessageCoro:
       parse ─ (co_await RDMA read attachment, 本核io_uring)
       ─ CacheNode::RangeCoro
            ├─ singleflight: co_await photon::semaphore        （本核让出/续跑，无跨核）
            ├─ 回源: AsyncGet + co_await semaphore             （SDK回调 signal 本核续跑）
            └─ 本地盘: co_await photon preadv（本核io_uring）  （含信号量限流）
       ─ co_await RDMA send response（本核io_uring）
   全程同核，I/O 完成事件本核续跑；执行体不挂起/不跨核唤醒
```

---

## 依赖与构建改动

DingoFS 第三方依赖统一通过 **dingoEureka 预编译产物**（`THIRD_PARTY_INSTALL_PATH`，默认 `~/.local/dingo-eureka`）+ `find_package` + `cmake/FindXxx.cmake` 引入（见 `CMakeLists.txt:46-87`、`:168`）。Photon 沿用同一套机制，**vendoring 仅作为本地开发的兜底**。

### 方案一（推荐）：纳入 dingoEureka 预编译 + FindPhoton.cmake

1. 在 dingoEureka 构建清单中加入 PhotonLibOS（编译为 `libphoton.a`，开启 io_uring 后端），产物落到 `THIRD_PARTY_INSTALL_PATH/{include,lib}`。Photon 依赖 liburing（已有 `find_package(uring)`，`CMakeLists.txt:246`）、aio、ssl，均已在依赖集中。
2. 新增 `cmake/FindPhoton.cmake`（仿 `cmake/Finduring.cmake`）：
   ```cmake
   # cmake/FindPhoton.cmake
   find_path(photon_INCLUDE_DIR NAMES photon/photon.h)
   find_library(photon_LIBRARIES NAMES libphoton.a photon)
   include(FindPackageHandleStandardArgs)
   find_package_handle_standard_args(Photon DEFAULT_MSG photon_LIBRARIES photon_INCLUDE_DIR)
   if(Photon_FOUND AND NOT TARGET photon::photon)
     add_library(photon::photon UNKNOWN IMPORTED)
     set_target_properties(photon::photon PROPERTIES
       INTERFACE_INCLUDE_DIRECTORIES "${photon_INCLUDE_DIR}"
       INTERFACE_LINK_LIBRARIES "uring::uring"
       IMPORTED_LOCATION "${photon_LIBRARIES}")
   endif()
   ```
3. 在顶层 `CMakeLists.txt`（紧接 `:246 find_package(uring)` 之后）加：
   ```cmake
   find_package(Photon REQUIRED)
   message("Using Photon ${Photon_VERSION}")
   ```
4. 新增 `src/cache/photon/CMakeLists.txt`（仿 `src/cache/local/CMakeLists.txt`），产出 `cache_photon`，链接 `photon::photon uring::uring cache_common cache_iutil`；让 `cache_local` / `cache_node` / `cache_infiniband` 依赖 `cache_photon`。

### 方案二（兜底）：vendoring 子模块

把 PhotonLibOS 作为 git submodule 放 `third-party/photon`，用 `ExternalProject_Add` / `add_subdirectory` 编进来。仅在 dingoEureka 尚未就绪时用于本地验证，最终统一收敛到方案一。

### C++ 标准说明

`CMakeLists.txt:27` 当前 `CMAKE_CXX_STANDARD 17`，而项目规范要求 C++20。Photon 为**有栈协程，不依赖 C++20 coroutine（`co_await` 关键字）**，故本改造**不需要**先升 C++20，可在 C++17 下落地（降低引入风险）；文中的 `co_await` 为示意语义，实际写法是 `sem.wait()` / photon I/O 调用。后续如升 C++20，可平滑替换为语言级协程包装，不影响本设计。

---

## 文件结构

```
src/cache/
├── photon/                         # 新增：Photon 运行时接入层
│   ├── photon_env.h                # PhotonEnv：每核 vcpu 池、Submit/CurrentVcpu
│   ├── photon_env.cc               # vcpu pthread 主循环、绑核、mpsc 投递队列
│   ├── bridge.h                    # bthread↔Photon / brpc↔Photon 桥接（信号、异步回调适配）
│   ├── bridge.cc
│   ├── semaphore.h                 # 对 photon::semaphore 的薄封装（限流/singleflight 复用）
│   └── CMakeLists.txt              # 产出 cache_photon，链接 photon::photon
│
├── local/
│   ├── aio.h                       # 改：Aio::Wait/Run → photon 信号；或整体被 photon io_uring 取代
│   ├── aio_queue.{h,cc}            # 改：去掉 bg_wait_thread_ 与 use_pthread 执行队列（坍缩为本核 io_uring）
│   ├── io_uring.{h,cc}             # 改/复用：fixed_buffers 迁移到 photon registered buffer
│   └── local_filesystem.cc        # 改：AioRead/AioWrite → 协程版（co_await photon preadv/pwritev）
│
├── common/
│   ├── storage_client.{h,cc}       # 改：TaskClosure::Wait → co_await；Range/Put 协程版
│   └── task_tracker.h              # 改：DownloadTask::Wait/Run → photon::semaphore
│
├── iutil/
│   └── inflight_tracker.h          # 改：cond_.wait → photon::semaphore（限流信号量）
│
├── infiniband/
│   └── server_session.cc           # 改：OnNewMessage → PhotonEnv::Submit；去掉 BlockingClosure
│
└── node/
    └── node.cc                     # 改：RetrieveWholeBlock/RunTask/WaitTask 协程化

cmake/
└── FindPhoton.cmake                # 新增：仿 Finduring.cmake

CMakeLists.txt                      # 改：find_package(Photon REQUIRED)
```

---

## 实现步骤

按"先搭运行时、再逐点替换、最后清理后台线程"的顺序，每步可独立编译、灰度回退：

1. **引入 Photon 依赖**：dingoEureka 加 PhotonLibOS 产物 + `cmake/FindPhoton.cmake` + 顶层 `find_package(Photon)`。验证一个最小可执行能 `photon::init/fini` 并跑通本核 io_uring preadv。
2. **搭 `src/cache/photon/`**：实现 `PhotonEnv`（每核 vcpu、绑核复用 `utils/numa_binder`、mpsc 投递队列）、`bridge`（photon::semaphore + brpc 异步回调桥接）、`semaphore` 薄封装。补单测：跨线程 `signal/wait`、`Submit` 投递、vcpu 不被 bthread 阻塞调用卡死的断言。
3. **改阻塞点 5（本地盘 AIO）**：`LocalFileSystem::AioRead/Write` 提供协程版（`co_await` 本核 io_uring），保留旧路径走开关。验证延迟/吞吐与正确性后，移除 `aio_queue.cc` 的 `bg_wait_thread_` 与 `use_pthread` 执行队列。
4. **改阻塞点 4（限流）**：`InflightTracker` → `photon::semaphore`，与步骤 3 协同（AIO 路径的 iodepth 限流改 per-vcpu 信号量）。
5. **改阻塞点 2（回源）**：`StorageClient::Range/Put` 协程版（`AsyncGet/Put` + photon 信号），去掉 `TaskClosure` 的 bthread CV 与 `use_pthread` 隔离队列。
6. **改阻塞点 3（singleflight）**：`DownloadTask` → `photon::semaphore`，`CacheNode::RetrieveWholeBlock/RunTask/WaitTask` 协程化。
7. **改阻塞点 1（RDMA 入口）**：`ServerSession::OnNewMessage` → `PhotonEnv::Submit` 把整条请求处理投到本核 vcpu；`HandleNewMessage` 协程化，删除 `BlockingClosure`/`ProcessRequest` 的 `CountdownEvent` 等待；保留兼容期内对 brpc service 的异步桥接。
8. **清理与收敛**：删除已无用的 `RunInBthread`+`BthreadJoiner` 在数据路径的使用、`bg_wait_thread_`、各 `use_pthread` 执行队列；统一 vcpu 数 = brpc worker 数 = 绑核数。

---

## 兼容性与灰度

- **全程开关化**：新增 `FLAGS_cache_use_photon_rtc`（默认 false）。每个阻塞点的协程版与旧 bthread 版并存，开关选择；按 5 → 4 → 2 → 3 → 1 顺序逐点放量，任一点异常可单独回退该点。
- **控制面不动**：仅改造数据路径（RDMA 请求处理 + 回源 + 本地盘 I/O）。元数据 RPC、心跳、MDS 交互等仍走 bthread/brpc，通过"方向 B"异步桥接被 Photon 协程调用，行为不变。
- **接口签名兼容**：`StorageClient::Range/Put`、`LocalFileSystem::AioRead/Write` 保留原同步签名作为 thin wrapper（开关关时走旧路径，开时 `PhotonEnv::Submit` 到本核 vcpu 跑协程版并桥接回结果），上层调用方无需改动即可灰度。
- **绑核策略对齐**：Photon vcpu 与 brpc worker 共用 `utils/numa_binder` 的绑核计划，确保"本核"概念在两套调度器间一致；未绑核环境下退化为不绑核但仍 per-vcpu。

---

## 风险与对策

| 风险 | 说明 | 对策 |
|------|------|------|
| 跨界阻塞卡死 vcpu | 在 Photon vcpu 上误调 bthread/brpc 同步原语会卡死整个 vcpu（连带本核所有协程） | 在 `bridge` 层做断言/封装：vcpu 内禁用 bthread 阻塞 API；CI 加静态检查（grep `bthread::`、同步 brpc）；code review 卡点 |
| 唤醒跨核 | brpc/ SDK 回调线程 `signal` 时把协程排到非本核 vcpu，破坏 RTC | 桥接固定唤醒到"发起协程的那个 vcpu"（记录 origin vcpu_idx，`Submit(origin, resume)`） |
| Photon 与 brpc 共进程冲突 | 信号、`epoll`/io_uring fd、TLS、glog/gflags 全局符号潜在冲突 | 先做共存冒烟测试（Photon vcpu + brpc server 同进程跑稳）；Photon 用 io_uring 后端避开与 brpc epoll 抢全局 reactor |
| io_uring 内核版本/能力 | fixed buffer、SQPOLL、registered fd 在不同内核行为不一致（现状 `IOUringOptions.use_sqpoll=true`） | 复用现有 `IOUring::Supported()` 探测逻辑；不支持时该核退回旧 AIO 路径（开关粒度到核） |
| 限流语义变化 | inflight 从全局改 per-vcpu，总并发 = 单核上限 × vcpu 数 | 明确 iodepth 语义为 per-vcpu（与本核 io_uring 深度一致）；需全局上限时用 vcpu-safe 全局信号量 |
| 内存分配争用 | Photon 不接管 malloc（这正是选它的原因），但热点路径分配仍走 tcmalloc | 复用现有 `USE_TCMALLOC`；IOBuffer / RDMA buffer 走现有 buffer pool，协程化不新增热点分配 |
| 调试可观测性 | 协程栈与 bthread 栈混合，排障复杂 | 每核 vcpu 暴露 bvar（在跑协程数、io_uring inflight、本核 latency）；保留旧路径作 A/B 对照基线 |

---

## 测试方案

1. **单元测试**
   - `src/cache/photon/`：`PhotonEnv` vcpu 启停/绑核；`Submit` 投递正确性；`bridge` 跨线程 `signal/wait`、brpc 异步回调唤醒；"vcpu 不被某协程的 photon I/O 长占"的公平性；**负向**：断言在 vcpu 上调 bthread 阻塞会被拦截。
   - 各阻塞点改造后保留并扩展原有单测（AIO 读写正确性、限流上限、singleflight 去重唯一回源、回源错误/重试路径），开关开/关两套都跑。
2. **正确性对照（开关 A/B）**：同一组读写负载，`FLAGS_cache_use_photon_rtc` 开/关两次跑，比对返回数据 bit 级一致、错误码一致、singleflight 去重次数一致。
3. **性能基准**：用 `tests/perf` 风格基准 + 现有缓存压测，测量：单请求端到端延迟（重点看 p99，应随跨核唤醒消除而下降）、吞吐、CPU 跨核迁移次数（`perf c2c` / 调度统计）、上下文切换次数（应显著下降）。对照 5 个阻塞点逐点放量的延迟曲线。
4. **共存稳定性**：Photon vcpu + brpc server + RDMA 同进程长稳（≥24h）压测，观察无死锁、无 fd 泄漏、内存平稳。
5. **故障注入**：回源超时/错误、io_uring 提交失败、内核不支持 io_uring（退回旧路径）、follower 等待超时（`FLAGS_retrieve_storage_lock_timeout_ms`）等分支在协程版下行为与旧版一致。
6. **灰度回退演练**：运行中切换开关（逐点回退），验证无 inflight 请求丢失、无悬挂协程。
