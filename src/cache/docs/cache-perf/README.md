# DingoFS 缓存服务端性能优化设计（thread-per-core / RTC）

本目录是 DingoFS 缓存服务端（`dingo-cache`）一组性能优化的**设计文档集**。目标：在尽量保留现有零拷贝数据面的前提下，分阶段消除控制面的串行瓶颈，并最终把热数据路径改造成 **thread-per-core / run-to-completion（RTC）** 模型，大幅提升吞吐与降低尾延迟。

## 核心判断

经过对读路径、写路径、线程/IO 模型、网络入口、内存的逐行调研，结论是：

> **DingoFS 缓存的「数据面」已经很好了**——RDMA 单边读写零拷贝、IOBuf user-data 零拷贝、lock-free slab 内存池、io_uring + O_DIRECT + SQPOLL + 注册 buffer。**性能损失在「控制面」**：一把全局 LRU 大锁、每盘单 ring + 跨线程阻塞的 IO 模型、上传时把刚写的块回读盘的 IO 放大、以及完全没有核亲和导致共享状态在所有核间弹跳。
>
> **因此不需要从零造 Seastar，第一阶段甚至不需要 thread-per-core。** 按性价比，先拆几个最痛的串行点即可拿到大头收益；要冲性能天花板再上完整 RTC（用 **Photon** 而非 Seastar，因为 Seastar 要独占 main/接管 malloc，无法与现有 brpc + bthread + RDMA 共存）。

## 瓶颈地图（均有源码证据，详见各文档）

| 瓶颈 | 证据 | 性质 |
|------|------|------|
| 🔴 全局 LRU 大锁 | `DiskCacheManager::mutex_`，源码自带 `// FIXME: lock contention`（`disk_cache_manager.cc:164`）| 每次读/写/淘汰都抢的最热跨核串行点 |
| 🔴 每盘单 ring + 跨线程阻塞 IO | 每盘 1 ring + 1 提交 pthread + 1 完成 pthread；业务 bthread 在 `aio.Wait()` condvar 挂起（`aio.h:66`）| "提交→阻塞→他线程唤醒"，与 RTC 相反 |
| 🟠 上传回读盘放大 | `DoUpload` 把刚写的块从盘读回再上传（`block_cache_uploader.cc`）| 多余的一次 4MB 读 |
| 🟠 后台上传单线程 + 全局队列 | 1 个 UploadWorker + 全局 PendingQueue + 10ms 空轮询 | 所有核漏斗到一线程一锁 |
| 🟠 无核亲和 | 只有进程级 NUMA，无 pinning；同 key 落任意核 | 共享状态在所有核间 cache-line 弹跳 |

**已经很好、应保留**：RDMA 零拷贝数据通路、`AppendUserDataWithMeta` 零拷贝、lock-free slab `MemoryPool`、MemCache 的 32-shard 设计（可当分片范式）。

## 方案导航

| # | 文档 | 阶段 | 性价比 | 改造面 |
|---|------|------|--------|--------|
| 01 | [分片磁盘缓存索引锁](./01-shard-diskcache-lock.md) | P1 | ⭐⭐⭐⭐⭐ | `disk_cache_manager.*` |
| 02 | [per-core / 多 io_uring ring](./02-per-core-io-uring.md) | P1 | ⭐⭐⭐⭐ | `aio_queue.*`/`io_uring.*`/`local_filesystem.cc` |
| 03 | [消除上传回读盘](./03-eliminate-upload-readback.md) | P1 | ⭐⭐⭐⭐ | `block_cache_uploader.*`/`disk_cache.cc` |
| 04 | [上传流水线并行化](./04-parallel-upload-pipeline.md) | P1 | ⭐⭐⭐ | `block_cache_uploader.*` |
| 05 | [key→核 亲和路由](./05-key-core-affinity-routing.md) | P2 | ⭐⭐⭐ | `infiniband/server_session.*`/`event.*`/`remote/*` |
| 06 | [绑核 + 每核事件循环](./06-per-core-event-loop.md) | P2 | ⭐⭐⭐ | 新增 per-core runtime / `dingo_cache.cc` |
| 07 | [共享状态全面 per-core 化（总纲）](./07-shared-state-sharding.md) | P2 | ⭐⭐⭐ | 横切（trackers/singleflight/连接池/slab） |
| 08 | [全面去阻塞 / RTC（Photon）](./08-deblocking-rtc-photon.md) | P2 | ⭐⭐⭐⭐ | 横切（所有阻塞点）+ 引入 Photon |

> 对应 Q902 改造清单的 9 项：item 1→01，2→02，3→03，4→04，5→05，6→06（含 item 9：Cache/Prefetch 不再每请求起 bthread），7→07，8→08。

## 阶段与依赖关系

```
Phase 1（独立可发布，不动整体架构，先做，先量收益）
  01 分片 LRU 锁 ── 互不依赖 ── 02 多 ring ── 03 去回读 ── 04 上传并行
        │                                            └── 03 与 04 协同（同属上传流水线）
        ▼  （Phase 1 测下来仍不够，再上 Phase 2）
Phase 2（真 thread-per-core / RTC，彼此强耦合，建议一体推进）
  08 去阻塞/Photon ──┬── 06 每核事件循环 ──┬── 05 key→核亲和路由
                     └── 07 共享状态 per-core 化 ┘
  （06/07/08 是一体的：先有 08 协程化去阻塞，才能在 06 的绑核事件循环里
    安全运行；05 把请求按 key 喂给对应核；07 把状态按核切分支撑无锁运行。）
```

**推荐落地顺序**：
1. **先做 03**（消除回读盘）—— 改动最小、纯赚、零架构风险，立即上线。
2. **再做 01**（分片 LRU 锁）—— 最热的锁，参照 MemCache 32-shard，收益最直接。
3. **接着 02 + 04**（多 ring + 上传并行）—— 拆掉 IO 与上传的单线程漏斗。
4. **量一轮**。若 Phase 1 已满足目标，可暂停；P2 是更大投入。
5. **要冲天花板**：按 08 → 06/07 → 05 的顺序推进 Phase 2（用 Photon 增量改造，与 brpc 共存）。

## 重点原则（贯穿所有方案）

1. **别碰数据面，只改控制面。** 零拷贝 RDMA + slab 是资产；改异步后务必复核"slab/IOBuf 在 in-flight 期间 refcount≥1 不被提前释放"。
2. **先量再改，每阶段都测。** 先用 perf/火焰图确认热点确在 LRU 锁与 IO 等待上，每个改造给出改前/改后对比数字。
3. **阻塞是 thread-per-core 的头号杀手。** Phase 2 绑核前必须先用 08 完成去阻塞，否则一次阻塞卡死整个核、性能更差。
4. **RTC 用 Photon 不用 Seastar**（见 [seastar 知识库 Q900/Q902 的结论](../../../../../seastar/.docs/QA/09_selection.md)）。

## 来源

本设计集源自对 Seastar thread-per-core/RTC 模型的调研与 DingoFS 缓存代码的逐行分析。背景与选型论证见 seastar 知识库 Part 9（选型与实践）的 Q900 / Q902。
