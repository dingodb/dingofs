# 节点内 key→核 亲和路由设计

## 概述

本文档描述在 DingoFS 缓存节点（Cache Node）内部新增一层 **`block-key → shard → 固定核（execution_queue）`** 的二次分派机制，使得**同一个 block key 在同一节点内始终落到同一个固定的执行单元（核）上处理**。

当前 DingoFS 已经在 **client 侧** 通过 ketama 一致性哈希实现了 `key → 节点` 的亲和（`RemoteCacheCluster::SelectNode`），但请求进入目标节点之后，节点内部的处理是**无 key 亲和**的：

- brpc/TCP 入口由 bthread 全局调度，且 bthread 支持 work-stealing，请求会在不同 pthread / 核之间迁移；
- RDMA 自研 server 在 `HandleNewMessage` 时对每条消息都 `RunInBthread` 起一个新 bthread，同样由 bthread 全局调度。

节点内缺乏 key 亲和带来的代价是：同一个 block key 的连续读写（如 Range 命中后的 Put/Cache、热点对象的并发访问）会散落到不同核上，导致 **per-key 元数据 / 索引结构 / 本地缓存分片锁的跨核竞争与 cacheline 抖动**。本设计的目标是把"同 key 同核"作为节点内的默认行为，把锁竞争退化为**单核串行**或**近无锁的 per-shard 结构**。

核心结论：**RDMA 入口是这一层二次分派的更合适载体**——它是自研、可控、且本身已有清晰的"每消息分派点"；而 TCP/brpc 那条链路上做核亲和需要与 brpc 的 bthread 调度器对抗，代价高、收益不确定，本设计**仅在 RDMA 入口落地，brpc 入口保持现状**。

---

## 背景与动机

### client 侧已有 key→节点 亲和

client 在 `RemoteCacheCluster::SendRequest` 中用 block key 的 `Filename()`（即 `{id}_{index}_{size}`）做一致性哈希选节点：

- `src/cache/remote/remote_cache_cluster.cc:188`：
  ```cpp
  auto node = node_group->SelectNode(FromHandlePB(request.raw.handle()).Filename());
  ```
- `src/cache/remote/remote_cache_cluster.h:48`：`SelectNode` 内部调用 `chash->Lookup(key, node)`。
- `src/cache/iutil/ketama_con_hash.cc:72`：`KetamaConHash::Lookup` 用 MD5 摘要在 continuum 上 `lower_bound`，是标准 ketama 实现（每节点 160 个虚拟点，见 `ketama_con_hash.cc:30-31`）。

所以**第一跳（哪个节点）已经是 key 确定的**。

### 节点内连接是轮询，不是 key 亲和

client 选定节点后，在节点内部选连接走的是**轮询（round-robin）**：

- `src/cache/remote/remote_node.h:71`：
  ```cpp
  RemoteNodeConnection* GetConnection() {
    return connections_[next_conn_index_.fetch_add(1) % FLAGS_connections].get();
  }
  ```

也就是说，同一个 key 的多次请求会被均匀打散到该节点的 N 条连接上，**连接层面已经丢失了 key 信息**。

### 节点内无 key→线程/核 映射

进入服务端后，两条入口都没有 key→核 的映射：

- **brpc/TCP 入口**：`src/cache/node/service.cc` 的 `BlockCacheServiceImpl::Put/Range/Cache/...` 由 brpc 框架在 bthread 上回调。bthread 是 M:N 用户态协程，且 brpc 默认开启 work-stealing，bthread 会在 worker pthread 之间迁移，**无法保证同 key 同核**，框架内部也没有暴露稳定的"按 key 选执行单元"的钩子。
- **RDMA 自研 server**：`src/cache/infiniband/server_session.cc:150` 的 `OnNewMessage` 对每条收到的消息：
  ```cpp
  auto tid = iutil::RunInBthread(
      [this, recv_buffer]() { HandleNewMessage(recv_buffer); });
  ```
  `RunInBthread`（`src/cache/iutil/bthread.cc:42`）调用 `bthread_start_background` 起一个 `BTHREAD_ATTR_NORMAL` 的 bthread，同样进入 bthread 全局调度，无 key 亲和。

### RDMA 入口有天然的分派点

RDMA 路径的事件流是分层、可控的：

1. `src/cache/infiniband/event.cc:156` `EventDispatcher::EventWorker`：独立 epoll 线程，`epoll_wait` 拿到就绪 fd 后调用 `handler->HandleEvent()`。
2. `src/cache/infiniband/event.cc:63` `GetGlobalEventDispatcher(fd)`：**已经按 fd 做了一次分派**——当 `rdma_event_dispatcher_num > 1` 时用 `butil::fmix64(fd) % N` 选 dispatcher（`event.cc:43` 默认 `rdma_event_dispatcher_num = 1`）。注意这是 **per-fd（per-connection）** 分派，不是 per-key。
3. `src/cache/infiniband/server_session.cc:86` `ServerSession::HandleEvent` → `conn_->HandleCompletion` → `execution_queue_execute(queue_id_, cqes)`：先把一批 CQE 投到 session 自己的 `ExecutionQueue`（`server_session.h:93`）。
4. `src/cache/infiniband/server_session.cc:95` `HandleWorkCompletion` 在该队列里回调 `ctx->on_completion`，进而 `OnNewMessage` → `RunInBthread(HandleNewMessage)`。

**关键插入点就是步骤 4 中 `RunInBthread` 之前**：在这里我们已经持有 `recv_buffer`，可以低成本地从中解析出 block key，然后据此选定固定核，而不是无脑起一个全局调度的 bthread。

### block key 的可用性（重要约束）

block key 由 `id / index / size` 组成，`Filename()` = `{id}_{index}_{size}`（`src/common/block/block_key.h:40`）。但在 RDMA wire format 里：

- `RequestMeta`（`proto/dingofs/infiniband.proto:38`）只含 `service_name / method_name / attachment_size / read_regions / write_region`，**不含 block key**。
- block key 在**业务请求 protobuf 的 data 段**里：`PutRequest.handle.block_key`（`proto/dingofs/blockcache.proto:24/40/47`），需要解析业务 `request` 才能拿到。
- 而业务 `request` 的解析发生在 `RequestParser::Parse`（`src/cache/infiniband/protocol.cc:205`）里，目前是在 `HandleNewMessage` 内、即**已经在 per-message bthread 上**才做。

这意味着**分派点拿不到现成的 key，需要在分派路径上先做一次"轻量 key 提取"**。本设计据此把"解析 meta + 提取 key"拆到分派前，详见下文。

---

## 当前实现分析

### RDMA 入口的处理链路（现状）

```
epoll 线程 (EventDispatcher::EventWorker, event.cc:156)
   │  epoll_wait 返回就绪 fd
   ▼
ServerSession::HandleEvent (server_session.cc:86)
   │  conn_->HandleCompletion -> execution_queue_execute(queue_id_, cqes)
   ▼
HandleWorkCompletion (server_session.cc:95, 在 session 的 ExecutionQueue 上)
   │  for each wc: ctx->on_completion(wc)
   ▼
OnNewMessage (server_session.cc:150)
   │  RunInBthread(HandleNewMessage)   ← 每消息一个 bthread，全局调度，无 key 亲和
   ▼
HandleNewMessage (server_session.cc:166, 运行在任意 worker pthread/核)
   │  ParseRequest  (protocol.cc:180, 此处才解析出 block key)
   │  ReadAttachment / ProcessRequest / SendResponse
```

观察点：

1. **现有分派粒度是 per-connection（fd）**，不是 per-key。一条连接上的所有 key 共享同一个 session ExecutionQueue，再被打散到全局 bthread 池。
2. **key 解析时机偏晚**：在 `HandleNewMessage` 内的 `ParseRequest` 才有 key，而那时已经选完了执行单元（bthread）。要做 key→核 亲和，必须把"够用的 key 信息"提前到分派决策点。
3. `ProcessRequest`（`server_session.cc:247`）用 `BlockingClosure` 同步等待 `service->CallMethod` 完成——也就是说**每条消息的实际业务处理是在它所在的 bthread 上串行完成的**。这对本设计很友好：只要把"某 shard 的所有消息"固定到同一个执行单元，shard 内天然串行。

### brpc/TCP 入口（现状）

`service.cc` 的回调由 brpc 调度，DingoFS 无法控制其落在哪个核；且 brpc bthread work-stealing 会主动迁移。要在这条链路上实现 key→核 亲和，只能：
- 在回调里再把任务转投到自建的 per-shard 队列（等于在 brpc 之上又套一层调度，引入二次排队 + 上下文切换 + 拷贝），或
- 改 brpc 调度策略（侵入第三方、风险大）。

两者代价都高，收益还会被 brpc 自身的调度抵消，因此**本设计不在 brpc 入口实现核亲和**。

---

## 设计方案

### 总体思路

在 RDMA 入口新增 **节点内 KeyCoreRouter**：一组**固定数量、各自绑定到一个固定核**的 `ExecutionQueue`（称为 **shard**）。每条 RDMA 消息在分派前提取 block key，用 `shard = hash(key) % num_shards` 选定 shard，把该消息的处理任务投递到对应 shard 的队列。由于每个 shard 队列固定在一个核上**串行**执行，**同 key → 同 shard → 同核**自然成立。

```
                       ┌──────────────────────────────────────────┐
                       │            Cache Node (RDMA 入口)          │
   client (ketama)     │                                          │
   key → node ─────────┤  epoll 线程                               │
                       │     │ HandleEvent                         │
                       │     ▼                                     │
                       │  session ExecutionQueue (per-conn)        │
                       │     │ HandleWorkCompletion                │
                       │     │  ├─ PeekKey(recv_buffer)  ← 轻量提取 │
                       │     │  └─ shard = hash(key) % N            │
                       │     ▼                                     │
                       │  ┌─────────┬─────────┬─────────┐          │
                       │  │ shard 0 │ shard 1 │ ...      │  每 shard│
                       │  │ EQ@core0│ EQ@core1│          │  绑定一核│
                       │  └────┬────┴────┬────┴────┬─────┘          │
                       │       ▼         ▼         ▼               │
                       │   HandleNewMessage (串行 / per-shard)      │
                       └──────────────────────────────────────────┘
```

### 分派位置与 key 提取

把 `OnNewMessage` 里"无脑 `RunInBthread`"替换为"提取 key → 选 shard → 投递到 shard 队列"。由于 key 在业务 data 段，需要一次轻量提取：

1. **轻量 PeekKey**：新增 `Protocol::PeekBlockKey(const RDMABuffer*, BlockKey*)`，复用 `ParseHeader`（`protocol.cc:88`，O(1)，只校验 magic / 长度），然后**只反序列化 data 段对应方法的 handle 字段**。两种实现可选，按落地成本择一：
   - **方案 A（推荐，零业务耦合）**：在 RDMA wire 协议的 `RequestMeta` 里**新增一个 `bytes routing_key`（或 `uint64 routing_hash`）字段**，由 client 在 `RequestSerializer`（`protocol.cc:165`）填入 `BlockKey::Filename()` 或其 64-bit 哈希。服务端分派时只需解析很小的 meta（`RequestMeta`，已在分派路径附近解析），**完全不必触碰业务 data 段**，最省、最稳。
   - **方案 B（不改协议）**：服务端在分派点对 data 段做一次"部分解析"——按 `method_name` 取出 `handle.block_key`。但 protobuf 部分解析仍需解析整个 message，成本与完整 `ParseRequest` 接近，且把业务类型耦合进 infiniband 层，不推荐。

   **本设计采用方案 A。** `routing_key` 的语义与 client 侧 ketama 完全一致（都用 `BlockKey::Filename()` 派生），保证跨层一致。

2. **选 shard**：
   ```cpp
   uint64_t h = HashKey(routing_key);          // 复用 fmix64 或 MD5 低 64 位
   uint32_t shard = h % FLAGS_rdma_key_shard_num;
   ```
   `HashKey` 直接用 `butil::fmix64`（`event.cc:25/68` 已在用，零额外依赖）对 `routing_hash` 混合即可；若 client 直接传 `routing_hash`，服务端连哈希都省了。

3. **投递到 shard 队列**，替换 `OnNewMessage` 里的 `RunInBthread`：
   ```cpp
   void ServerSession::OnNewMessage(const WorkCompletion& wc, RDMABuffer* recv_buffer) {
     if (!wc.status.ok()) { ... return; }
     recv_buffer->length = wc.byte_len;

     uint64_t routing_hash = 0;
     if (Protocol::PeekRoutingHash(recv_buffer, &routing_hash).ok()) {
       router_->Dispatch(routing_hash,
                         [this, recv_buffer] { HandleNewMessage(recv_buffer); });
     } else {
       // 无 routing key（如非 block 类请求 / 老 client）：回退到原有全局 bthread
       auto tid = iutil::RunInBthread([this, recv_buffer] { HandleNewMessage(recv_buffer); });
       if (tid != 0) joiner_->BackgroundJoin(tid);
     }
   }
   ```

### KeyCoreRouter / shard 队列

新增 `KeyCoreRouter`，进程级单例（与 `GetGlobalEventDispatcher` 同生命周期），所有 session 共享同一组 shard——**这是"同 key 同核"在整节点成立的前提**（若每 session 各建一组 shard，跨连接的同 key 仍会分散）。

- 每个 shard 持有一个 `bthread::ExecutionQueue<Task>`，`ExecutionQueueOptions.use_pthread = true`，并**把承载该队列的 pthread 绑定到一个固定核**（`pthread_setaffinity_np` / `bthread` 的 worker 亲和，见下）。
- shard 数 `FLAGS_rdma_key_shard_num` 默认 = `min(物理核数, rdma_event_dispatcher_num 的上层并发预算)`，建议初值取 NUMA 本地核数；可灰度。
- `Dispatch(hash, task)`：
  ```cpp
  void KeyCoreRouter::Dispatch(uint64_t hash, std::function<void()> task) {
    auto& shard = shards_[hash % shards_.size()];
    int rc = bthread::execution_queue_execute(shard.queue_id, std::move(task));
    if (rc != 0) {                       // 队列满 / 停止 → 降级
      vars_.dispatch_fallback << 1;
      auto tid = iutil::RunInBthread(std::move(task));   // 回退到全局调度
    }
  }
  ```

**核绑定**：`ExecutionQueue(use_pthread=true)` 会在一个专用 pthread 上消费队列。在 shard 启动时对该 pthread 调用 `pthread_setaffinity_np` 绑定到 `shard_id` 对应的 CPU。这样"shard → 核"是静态、稳定的，且**不依赖 bthread work-stealing**（ExecutionQueue 的消费是单消费者、不被 steal 的）。这正是相比 brpc 路径的核心优势。

> 注意：`HandleNewMessage` 内部的 `ProcessRequest`（`server_session.cc:247`）使用 `BlockingClosure` **同步阻塞等待**业务完成。若直接放在 shard 队列里执行，会**阻塞该 shard 队列的消费**，使后续同 shard 消息排队。这正是我们想要的"shard 内串行"语义——但要求业务处理本身不能长时间阻塞 I/O。当前 `Range`/`Put` 走本地 cache store，属于可接受的短阻塞；若未来某方法可能长阻塞，应在该方法内部异步化（投递到独立 I/O 池后立即返回），shard 队列只负责"启动 + 路由"，不等待完成。本设计在第一阶段保持现状（shard 内同步），并在风险章节标注。

### 与 client 侧 ketama 的协同

- **两层哈希语义对齐**：client 用 `BlockKey::Filename()` 经 ketama 选**节点**；节点内用**同一个** key（经 `routing_hash`）选 **shard/核**。两层都以 block key 为唯一输入，保证"同 key 全程确定路径"：`同 key → 同节点 → 同核`。
- **独立哈希函数**：节点选择用 ketama（带虚拟节点、权重、节点增删平滑），shard 选择用 `fmix64 % N`（节点内 shard 数固定、不需要平滑迁移，简单取模即可）。两者无需相同算法，只需**输入相同**。
- **节点增删时**：ketama 重建只影响"key→节点"映射（`remote_cache_cluster.cc:308` `BuildHashRing`），节点内 shard 映射不变。某个 key 迁移到新节点后，在新节点内仍按其自身哈希落到某个 shard，行为自洽。

### 是否需要 client 侧也按 key 选连接？

**需要、但应作为可选增强。** 现状 client 侧 `RemoteNode::GetConnection`（`remote_node.h:71`）是轮询，这本身不破坏节点内 key→核 亲和（因为节点内分派只看 `routing_hash`，与走哪条连接无关）。但有两点值得让 **client 侧也按 key 选连接**：

1. **保序/缓存友好**：同 key 走同一条连接，TCP/QP 层的 inflight 顺序更可预期，连接级的发送缓冲/批量也更易聚合。
2. **配合 per-connection 的 session 队列**：RDMA 现有"per-fd 分派"（`event.cc:63`）与"per-shard 分派"是两层。如果 client 把同 key 固定到同连接，则 epoll 那一跳（`fmix64(fd)`）对同 key 也稳定，进一步减少跨 dispatcher 抖动。

建议把 `GetConnection` 改为**可按 key 选择**：
```cpp
RemoteNodeConnection* GetConnection(uint64_t routing_hash) {
  if (FLAGS_conn_select_by_key) {
    return connections_[routing_hash % FLAGS_connections].get();
  }
  return connections_[next_conn_index_.fetch_add(1) % FLAGS_connections].get();
}
```
默认关闭（保持轮询的均衡性），灰度验证后开启。**这一项是锦上添花，不是节点内核亲和成立的必要条件。**

### 热 key 导致核间负载不均的检测与降级

固定哈希分派的固有风险：**少数热 key 会把某个 shard（核）压满，而其它核空闲**。需要检测 + 降级：

1. **检测**——每个 shard 暴露 bvar 指标：
   - `shard_queue_depth{shard=i}`：队列积压长度（`execution_queue` 可近似用"已投递 - 已完成"计数）。
   - `shard_inflight_us{shard=i}`：单条消息处理耗时分布。
   - `shard_dispatch_count{shard=i}`：投递计数，用于算核间分布偏斜。
   控制面周期性计算偏斜度 `skew = max_depth / avg_depth`。

2. **降级策略（分级）**：
   - **L0（默认）**：严格同 key 同核。
   - **L1 副本扇出**：当某 shard `skew` 超阈值（如 `FLAGS_rdma_shard_skew_threshold = 4`）且持续 `T` 毫秒，对该 shard 的**只读请求（Range/Prefetch）**允许在 `{shard, shard^1, shard+1}` 这一小组**副本核**间轮转分派。只读请求跨核不破坏正确性（本地 cache store 读是幂等的），只牺牲一点 cache 局部性换吞吐。
   - **L2 全局回退**：当整体压力突增或 shard 队列投递失败（`execution_queue_execute` 返回非 0），单条消息直接回退到全局 bthread（`RunInBthread`），即退化为现状行为，保证**不丢请求、不卡死**。
   - **写请求（Put/Cache）**：默认**不跨核降级**（保持同 key 同核以维持 per-key 写串行的简单性）；仅在 L2 失败兜底时才回退全局。

3. **降级是自动 + 可观测的**：所有降级动作打 bvar（`dispatch_fallback`、`shard_replica_fanout`）和 `LOG_EVERY_SECOND`，便于定位热 key。

> 说明："允许跨核/副本"只对**读**安全地放开；对**写**保守。这与缓存语义一致：读可重复、跨核只损局部性；写需要 per-key 串行以简化一致性。

---

## 文件结构

```
src/cache/infiniband/
├── key_core_router.h          # 新增：KeyCoreRouter 声明（shard 数组 + Dispatch + 核绑定）
├── key_core_router.cc         # 新增：ExecutionQueue per-shard、pthread_setaffinity_np、bvar 指标、降级逻辑
├── protocol.h                 # 修改：RequestMeta 增加 routing_hash 字段相关序列化；新增 Protocol::PeekRoutingHash
├── protocol.cc                # 修改：RequestSerializer 填 routing_hash；PeekRoutingHash 实现（复用 ParseHeader）
├── server_session.h           # 修改：ServerSession 持有 KeyCoreRouter*（或访问全局单例）
├── server_session.cc          # 修改：OnNewMessage 用 router_->Dispatch 替换 RunInBthread，带回退
└── event.cc                   # 不改（per-fd 分派保持），可选：与 router 共享核拓扑

proto/dingofs/
└── infiniband.proto           # 修改：RequestMeta 增加 `uint64 routing_hash = 6;`

src/cache/remote/
├── remote_node.h              # 修改（可选）：GetConnection(routing_hash) 按 key 选连接
└── remote_node_connection.*   # 修改（可选）：RDMAConnection 序列化时填 routing_hash

src/common/options/cache.{h,cc}（或对应 gflags 定义处）
└── 新增 flags：
      rdma_key_shard_num            (default = NUMA 本地核数)
      rdma_shard_bind_core          (default = true)
      rdma_shard_skew_threshold     (default = 4)
      rdma_shard_replica_fanout     (default = true, 仅读)
      conn_select_by_key            (default = false, client 侧)
```

---

## 实现步骤

1. **协议扩展（最小、可独立合入）**
   - `proto/dingofs/infiniband.proto`：`RequestMeta` 增加 `uint64 routing_hash = 6;`。
   - `protocol.cc:165` `RequestSerializer::Serialize`：从上层 ctx 取 `routing_hash` 写入 meta。
   - `protocol.h/.cc`：新增 `Protocol::PeekRoutingHash(const RDMABuffer*, uint64_t*)`，内部复用 `ParseHeader` + 只 parse `RequestMeta`（meta 段很小）。
   - client 侧 `RDMAConnection::Send`（`remote_node_connection.cc`）把 `BlockKey::Filename()` 的哈希填进 ctx.routing_hash。
   - 兼容：老 client 不填则为 0，服务端按"无 routing key → 全局回退"处理（见步骤 3）。

2. **KeyCoreRouter（核心）**
   - 实现 `key_core_router.{h,cc}`：构造 `FLAGS_rdma_key_shard_num` 个 `ExecutionQueue<std::function<void()>>`（`use_pthread=true`），逐个 `pthread_setaffinity_np` 绑核（受 `rdma_shard_bind_core` 控制）。
   - `Dispatch(hash, task)`：取模选 shard，`execution_queue_execute`；失败计数并回退 `RunInBthread`。
   - 提供全局单例 `GetGlobalKeyCoreRouter()`，在节点启动（与 `InitializeGlobalDispatchers` 同处，`event.cc:54` 附近）初始化、`atexit` 关闭。
   - 接 bvar：`shard_queue_depth / shard_dispatch_count / dispatch_fallback`。

3. **改造 RDMA 分派点**
   - `server_session.cc:150` `OnNewMessage`：调用 `Protocol::PeekRoutingHash`；成功则 `router_->Dispatch`，失败/为 0 则保留原 `RunInBthread` 路径（**保证 100% 回退兼容**）。
   - `ServerSession` 持有 `KeyCoreRouter*`（构造时从全局单例注入）。
   - 保留 `joiner_` 仅用于回退路径上 `RunInBthread` 的 bthread join；shard 路径的任务由 ExecutionQueue 自行回收。

4. **热点检测与降级**
   - 在 `KeyCoreRouter` 内加偏斜计算（后台周期任务，复用 `BthreadExecutor` 或简单定时 bthread）。
   - 实现 L1 只读副本扇出：`Dispatch` 增加 `bool read_only` 入参，由 `method_name`（meta 已有）判定；热点时对只读请求在副本核小组内轮转。
   - 实现 L2 兜底：投递失败统一回退全局。

5. **client 侧按 key 选连接（可选增强，独立开关）**
   - `remote_node.h:71` `GetConnection` 增加按 `routing_hash` 选择分支，`FLAGS_conn_select_by_key` 控制，默认关闭。

6. **指标与运维**
   - 在节点 Dump（参考 `remote_cache_cluster.cc:240` 风格）或 bvar 页暴露 shard 分布、热点 key、降级计数。

---

## 兼容性与灰度

- **协议向后兼容**：`routing_hash` 是 proto3 新增可选字段，老 client 发 0，服务端落"无 key→全局回退"路径，**新老 client/server 可混跑**。
- **行为可关闭**：`rdma_key_shard_num = 1` 时，所有 key 落同一 shard——但这会把全部 RDMA 处理串行化到一个核，**不应作为关闭手段**。真正的"关闭开关"是让 `OnNewMessage` 走原 `RunInBthread` 分支：建议加 `FLAGS_rdma_key_core_affinity = false` 总开关，关闭时完全等价于现状，便于一键回退。
- **绑核可关闭**：`rdma_shard_bind_core = false` 时只做 shard 分派、不绑核（仍能减少跨核但不强制），用于不便绑核的环境（容器 cpuset 受限）。
- **灰度顺序**：
  1. 先合协议字段（无行为变化）。
  2. 单节点开 `rdma_key_core_affinity` + 小 `shard_num`，观察 bvar 偏斜与延迟。
  3. 逐步放大 shard 数到核数，开绑核。
  4. 最后灰度 client 侧 `conn_select_by_key`。
- **brpc 入口不受影响**：本设计只动 RDMA 链路，`service.cc` 与 TCP 路径零改动。

---

## 风险与对策

| 风险 | 说明 | 对策 |
| --- | --- | --- |
| 热 key 压垮单核 | 固定哈希 + 热点对象使某 shard 队列积压、延迟飙升 | bvar 检测偏斜；只读请求 L1 副本扇出；L2 全局兜底；写请求保守不跨核 |
| shard 内同步阻塞 | `ProcessRequest` 用 `BlockingClosure` 同步等业务完成（`server_session.cc:247`），长阻塞会卡住整个 shard | 第一阶段仅用于短阻塞（本地 cache 读写）；长阻塞方法在内部异步化，shard 队列只做"启动+路由"不等待 |
| key 提取额外开销 | 分派路径多一次 meta 解析 | 采用方案 A：只解析很小的 `RequestMeta`（meta 段），不碰业务 data 段；或 client 直接传 `routing_hash` 免哈希 |
| 绑核与环境冲突 | 容器 cpuset / NUMA 拓扑受限，`pthread_setaffinity_np` 失败 | `rdma_shard_bind_core` 可关；绑核失败降级为"不绑核仅分派"，不致命 |
| 跨层语义漂移 | client ketama 与节点内 shard 用不同哈希，若输入 key 不一致则亲和失效 | 两层强制以同一 `BlockKey::Filename()` 为输入；`routing_hash` 由 client 统一派生并随请求带下，杜绝服务端重算口径不一致 |
| 全局单例生命周期 | router 与 EventDispatcher、session 的启停顺序 | 复用 `InitializeGlobalDispatchers`/`atexit` 的既有模式（`event.cc:54-61`）统一初始化与回收 |
| 写请求跨核破坏串行假设 | 若降级把同 key 写打散到多核，per-key 写并发语义变复杂 | 写请求默认不参与 L1 副本扇出；仅 L2 失败兜底回退，且兜底是极端情况 |

---

## 测试方案

1. **单元测试**（`tests/unit/`，Boost.Test）
   - `Protocol::PeekRoutingHash`：构造含/不含 routing_hash 的 RDMA frame，校验提取正确、老格式返回"无 key"。
   - `KeyCoreRouter::Dispatch`：固定 key 集合反复 dispatch，断言"同 key 恒落同 shard"；模拟 `execution_queue_execute` 失败，断言走全局回退且计数 +1。
   - shard 绑核：在支持的环境断言消费 pthread 的 affinity mask 命中目标核（不支持则 skip）。

2. **并发/亲和性测试**
   - 多线程对同一组 key 并发发请求，统计每个 key 的实际处理核（在 `HandleNewMessage` 入口记录 `sched_getcpu()`），断言**同 key 处理核稳定**（L0 模式下方差为 0）。
   - 注入热 key（单 key 占比 >80%），断言触发 L1 副本扇出且只读请求被分散、写请求仍同核。

3. **回退兼容测试**
   - 老 client（routing_hash=0）打新 server：断言全部走 `RunInBthread` 路径、功能正确。
   - `rdma_key_core_affinity=false`：断言行为与改造前完全一致。

4. **性能基准**（`tests/perf/` 或 `apps/io_tester`）
   - 对比开关前后：高并发同 key 读写下的 P99 延迟、吞吐、跨核 cacheline miss（perf stat）。预期亲和开启后 per-key 锁竞争与 cacheline 抖动下降。
   - 偏斜场景下对比 L1 降级开/关的尾延迟。

5. **故障注入**
   - shard 队列满 / session 关闭中途分派：断言不丢请求、走兜底、无崩溃。
   - 节点成员变更（ketama 重建）期间持续打流：断言节点内 shard 路由稳定、无请求错路。
