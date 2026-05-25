# DingoFS `ReadMemPool` 实现设计

> 类型：实现设计  
> 项目：DingoFS  
> 状态：与当前代码同步  
> 更新时间：2026-05-25

## 1. 文档目的

本文档描述当前代码中的 `ReadMemPool` 实现，而不是早期方案草案。

这套实现的目标很明确：为 read 路径提供一块由上层主动申请、可复用、连续、4K 对齐的内存来源，让上层先拿到 buffer，再把这块内存交给下层填充数据。这样既能消除频繁 `malloc/free` 的抖动，也为后续零拷贝或 RDMA 注册保留了统一的地址空间基础。

当前实现已经落地在 `src/common/memory/` 下，代码不再是旧文档里的“单头单源文件自包含版本”，而是按职责拆成多个组件：`arena`、`buddy_allocator`、`slab_allocator`、`tls_cache`、`pool_slot`、`read_mem_pool`。

## 2. 当前代码布局

当前实现位于以下文件：

- `src/common/memory/arena.h`
- `src/common/memory/arena.cc`
- `src/common/memory/buddy_allocator.h`
- `src/common/memory/buddy_allocator.cc`
- `src/common/memory/slab_allocator.h`
- `src/common/memory/slab_allocator.cc`
- `src/common/memory/tls_cache.h`
- `src/common/memory/tls_cache.cc`
- `src/common/memory/pool_slot.h`
- `src/common/memory/pool_slot.cc`
- `src/common/memory/read_mem_pool.h`
- `src/common/memory/read_mem_pool.cc`
- `test/unit/common/test_read_mem_pool.cc`

职责拆分如下：

| 组件 | 职责 |
|---|---|
| `Arena` | 申请整块连续虚拟地址空间，保证基地址 2MB 对齐 |
| `BuddyAllocator` | 管理 4K 到 64MB 的 2 的幂连续块，负责 split / coalesce |
| `SlabAllocator` | 管理 `<=64K` 小块请求，固定使用 2MB slab block 做 region 切分 |
| `TlsCacheSet` | 为 buddy 的中小块提供线程侧快速缓存，减少全局锁竞争 |
| `PoolSlot` | 一个 move-only 的 RAII 句柄，代表“池里分配出来的一个 slot” |
| `ReadMemPool` | 对外门面，封装路由、回收和外部释放入口 |

## 3. 整体架构

整体结构如下：

```text
上层 read 路径
   |
   | Allocate(len)
   v
ReadMemPool
   |- len <= 64K --------------> SlabAllocator
   |                              |
   |                              v
   |                           BuddyAllocator 取 2MB slab block
   |
   |- len > 64K ---------------> TlsCacheSet 先尝试命中
                                  |
                                  v
                               BuddyAllocator
                                  |
                                  v
                                Arena
```

这里有三个关键点：

1. `Arena` 只做一次大块预分配，后续所有 slot 都来自这段连续地址空间。
2. `BuddyAllocator` 是整个系统的“底座分配器”，slab 和大块分配最终都落到它上面。
3. `ReadMemPool` 对上层只暴露非常小的接口：`Allocate`、`ReleaseExternal`、`Deleter`、`DrainTls`、`DrainIdle`。

## 4. 对外接口与使用语义

`ReadMemPool` 的核心公开接口如下：

- `PoolSlot Allocate(size_t len)`
- `void ReleaseExternal(void* p)`
- `std::function<void(void*)> Deleter()`
- `void DrainTls()`
- `void DrainIdle()`
- `uint8_t* BaseAddr() const`
- `size_t TotalSize() const`

使用语义是：

1. 调用方通过 `Allocate(len)` 拿到一个 `PoolSlot`。
2. `PoolSlot` 析构时会自动把这块 slot 还回池里。
3. 如果调用方要把这块内存交给外部对象继续持有，比如挂到 `IOBuf` 上，就调用 `Disown()` 交出所有权。
4. 外部对象最终通过 `ReadMemPool::Deleter()` 返回的 deleter 调 `ReleaseExternal(void*)` 归还这块内存。

这也是为什么当前类型名已经从早期文档里的 `PooledBuf` 收敛成了 `PoolSlot`：当前对象的核心职责不是“一个普通 buffer 值对象”，而是“代表一个 pool slot 的所有权句柄”。

## 5. `PoolSlot`：面向调用方的 RAII 句柄

`PoolSlot` 定义在 `pool_slot.h/.cc` 中，是当前实现里对外最重要的使用者语义。

它有以下特征：

- move-only，不可拷贝
- 持有 `pool_`、`data_`、`off_`、`cap_`、`meta_`
- 析构时调用 `ReadMemPool::ReturnSlot(off_, meta_)`
- `Disown()` 后句柄失效，不再负责回收

其中：

- `data_` 是这块连续内存的起始地址
- `cap_` 是实际容量，可能大于请求长度
- `off_` 是相对 `Arena::Base()` 的偏移
- `meta_` 编码了内存来源和 buddy order

`meta_` 的编码格式是：

```text
bit 7     : source, 0 = buddy, 1 = slab
bits 0..6 : order
```

对于 slab 路径，当前实现只用 `source=slab` 识别来源，`order` 不参与 slab 释放计算；对于 buddy 路径，`order` 会在 `ReturnSlot` 时直接用于归还。

## 6. `Arena`：连续地址空间底座

`Arena` 的职责很纯粹：申请并持有一整段连续地址空间。

当前实现的关键常量：

- `kArenaAlign = 2MB`

当前 `Create` 接口是：

```cpp
static std::unique_ptr<Arena> Create(size_t bytes, size_t align_to);
```

实际行为：

1. 先把 `bytes` 向上对齐到 `align_to`。
2. 优先尝试 hugepage `mmap`：`MAP_HUGETLB | MAP_POPULATE`。
3. 如果 hugepage 失败，则 fallback 到 `posix_memalign(2MB)`。
4. 无论哪条路径，最终都保证 `base` 是 2MB 对齐。

这里的设计含义是：

- 2MB 对齐不是“尽量”，而是当前实现的硬保证。
- hugepage 只影响页大小和预热方式，不影响接口语义。
- fallback 明确避免使用裸 `malloc`，因为那只能给出较小对齐，破坏 4K/O_DIRECT/RDMA 的前提。

`ReadMemPool` 当前构造时调用：

```cpp
arena_ = Arena::Create(budget_bytes, kMaxOrderBytes);
```

这意味着整个 arena 会被向上对齐到 64MB，即最大 buddy order 的块大小。

## 7. `BuddyAllocator`：通用连续块分配器

`BuddyAllocator` 是整个实现的核心分配器。

当前 order 常量如下：

- `kMinOrderShift = 12`
- `kMinOrderBytes = 4K`
- `kNumOrders = 15`
- `kMaxOrder = 14`
- `kMaxOrderBytes = 64MB`

也就是说，当前 buddy 覆盖的块大小范围是：

- order 0 = 4K
- order 1 = 8K
- ...
- order 14 = 64MB

`BytesToOrder(len)` 返回“最小的、能容纳 `len` 的 order”；如果 `len > 64MB`，返回 `-1`，当前实现直接视为 oversize，请求失败。

### 7.1 内部状态

当前实现的核心数据结构：

- `free_head_[kNumOrders]`：每个 order 一个空闲链表头
- `order_tag_`：每个 4K min-block 一字节 tag
- `used_`：当前 buddy 视角下已占用字节数
- `mu_`：全局互斥锁

其中 `order_tag_` 是正确性的关键。其编码方式为：

```text
0x80 | order : 该 min-block 起点是一个已分配块的起点
order        : 该 min-block 起点是一个空闲块的起点
0x80         : 非块起点的哨兵值
```

初始化时，`order_tag_` 先整体填成 `0x80`，然后把每个 64MB top block 的起点标记成 `14` 并挂入空闲链。这样可以避免 stale 读取误判成“某个 order 的整块空闲块”。

### 7.2 分配逻辑

分配流程：

1. 在目标 order 及更高 order 中寻找第一个非空空闲链。
2. 从高阶链表摘出一个块。
3. 若阶数过高，则不断 split。
4. 每次 split 时，把右半块挂回更低一阶的 free list。
5. 左半块继续往下拆，直到目标 order。
6. 最终把起点 tag 标成 `0x80 | order`，表示“已分配块起点”。

### 7.3 释放逻辑

释放流程：

1. 从当前块的 order 开始。
2. 用 `buddy = off ^ OrderBytes(order)` 算伙伴地址。
3. 只有当伙伴的 tag 恰好等于当前 `order` 时，才说明“伙伴是一个完整的、同阶空闲块”，允许合并。
4. 若可合并，则先从 free list 摘掉 buddy，再向上一阶继续尝试。
5. 最终把合并后的块挂回对应 free list。

这意味着 `order_tag_` 不是一个简单的“空闲/占用 bit”，而是 coalesce 正确性的一部分。

### 7.4 `OrderAt(off)` 的作用

`OrderAt(off)` 用于外部释放路径。

当 `PoolSlot` 正常析构时，order 来自句柄里的 `meta_`，不需要额外查表；但当内存已经 `Disown()` 并只剩一个裸指针时，`ReleaseExternal(void*)` 必须根据地址反查这块内存属于 slab 还是 buddy，以及 buddy 的 order。这里 buddy 的 order 就由 `OrderAt(off)` 提供。

## 8. `SlabAllocator`：小块路径

当前 slab 专门服务 `<= 64K` 的请求。

关键常量：

- `kSlabMax = 64K`
- `kNumSlabClasses = 5`
- `kSlabBlockBytes = 2MB`
- `kSlabBlockOrder = 9`
- `kSlabBlockShift = 21`

当前 size class 固定为：

- 4K
- 8K
- 16K
- 32K
- 64K

### 8.1 为什么 slab block 固定为 2MB

当前实现没有让 slab 按需选择不同 buddy order，而是统一从 buddy 申请一个固定 2MB 的块。这样做的直接收益是：

1. `region off -> slab block` 可以通过按位对齐快速得到 `block_off`。
2. `slab_owner_[off >> 21]` 就能 O(1) 找到所属 slab。
3. `ReadMemPool::ReleaseExternal` 可以先用 `OwnsBlock(off)` 判断来源，再决定是否调用 `slab_->Free(off)`。

这也是当前代码选择 `slab_owner_` 数组，而不是 `std::map<block_off, Slab*>` 的原因。

### 8.2 slab 内部结构

每个 `Slab` 记录：

- `block_off`
- `region_bytes`
- `total`
- `free_cnt`
- `free_head`
- `prev/next`

每个 `Bin` 记录：

- `region_bytes`
- `partial`：有空闲 region 的 slab 链表
- `empty_count`：当前这个 bin 中完全空闲 slab 的数量

当前 region free list 是侵入式的，空闲 region 的前 4 字节存储“下一个空闲 region index”。

### 8.3 分配与回收

分配逻辑：

1. 根据 `len` 找到 bin。
2. 若该 bin 没有 `partial` slab，则向 buddy 申请一个 2MB block，新建 slab 并切成 region。
3. 从 `partial` 链表头 slab 中摘出一个 region。
4. 若 slab 变满，则从 `partial` 链表移除。

回收逻辑：

1. 根据 `off` 反算所属 `block_off`。
2. 从 `slab_owner_` 找到对应 slab。
3. 把 region 挂回该 slab 的空闲链表。
4. 如果 slab 从“满”变成“部分空闲”，重新挂回 `partial` 链表。
5. 如果 slab 变成“完全空闲”，进入空 slab 保留策略。

### 8.4 当前空 slab 保留策略

当前代码实现了一个简单的 hysteresis：

- 每个 bin 最多保留 1 个完全空闲 slab block。
- 当某个 slab 变成空闲时，若该 bin 还没有空 slab，则保留它。
- 若该 bin 已经有 1 个空 slab，则把这个新空闲 slab 直接还给 buddy。
- `DrainEmpty()` 会把所有 bin 中保留的空 slab 全部还给 buddy。

这样做的目的，是避免小块 workload 在“刚分配一个 slab block，立刻全部释放，再次申请”这种模式下反复触发 2MB block 的向上/向下流动。

### 8.5 `OwnsBlock(off)` 的语义边界

`OwnsBlock(off)` 当前是无锁读取 `slab_owner_`，其注释明确限定了适用前提：

- 调用者传入的是“当前仍然有效、仍然持有中的已分配 region 对应地址”。

在这个前提下，这个 slab block 不会被并发回收清空，所以 `slab_owner_[idx]` 的值是稳定的。它不是一个任意地址通用判定 API，而是建立在对象生命周期不变量上的快速判断。

## 9. `TlsCacheSet`：buddy 的快速缓存层

TLS cache 只服务 buddy 的中小块请求，不覆盖 slab。

关键常量：

- `kNumCaches = 64`
- `kMaxLane = 8`
- `kTlsBudget = 1MB`

它不是“每线程一个真正的 thread_local cache 对象”，而是：

1. 池内固定持有 `caches_[64]`。
2. 每个线程第一次访问时，通过 `Slot()` 拿到一个 thread-local 的逻辑 slot id。
3. 线程实际使用 `caches_[slot % 64]`。

这意味着：

- 线程数量超过 64 时会共享 cache slot。
- slot 共享通过每个 cache 上的 `atomic_flag` try-lock 控制。
- 避免了真正 `thread_local` 容器在析构顺序上的问题，因为 cache 生命周期绑定到 `ReadMemPool`。

### 9.1 容量策略

`CapFor(order)` 的计算规则是：

```text
cap(order) = min(kMaxLane, kTlsBudget / OrderBytes(order))
```

如果某个 order 的块大小已经大于 `1MB`，则 `cap = 0`，该类完全不缓存，直接走 buddy。

因此：

- 4MB、8MB、16MB、32MB、64MB 这类大块不会进入 TLS cache。
- 256KB 这类中等块会被缓存。

### 9.2 填充与消费时机

TLS cache 的填充时机是 buddy 路径释放时：

1. `ReadMemPool::FreeBuddy(off, order)` 先调用 `tls_->TryFree(order, off)`。
2. 缓存成功则不再调用 buddy。
3. 缓存失败，才真正回到 `buddy_->Free(off, order)`。

消费时机是 buddy 路径分配时：

1. `ReadMemPool::Allocate(len)` 先根据 `len` 算 order。
2. 先 `tls_->TryAlloc(order)`。
3. miss 时再调用 `buddy_->Allocate(order)`。

因此，TLS cache 中的块在 buddy 看来仍然是“已分配”状态。只有 `DrainTls()` 或 `DrainIdle()` 时，才会真正把这些块回流给 buddy。

## 10. `ReadMemPool` 的真实路由逻辑

### 10.1 构造

构造流程：

1. `arena_ = Arena::Create(budget_bytes, kMaxOrderBytes)`
2. `buddy_ = std::make_unique<BuddyAllocator>(arena_->Base(), arena_->Total())`
3. `slab_ = std::make_unique<SlabAllocator>(buddy_.get(), arena_->Base(), arena_->Total())`
4. `tls_ = std::make_unique<TlsCacheSet>(buddy_.get())`

也就是说，slab 和 TLS 都建立在同一个 buddy 之上，而 buddy 又建立在同一个 arena 之上。

### 10.2 分配

当前 `Allocate` 的路由规则非常直接：

1. `!Valid()` 或 `len == 0`，返回空 `PoolSlot`。
2. `len <= kSlabMax`，走 slab。
3. `len > kSlabMax`，走 buddy 路径。
4. buddy 路径先试 TLS cache，再试 buddy。
5. `len > 64MB` 时，`BytesToOrder(len)` 返回 `-1`，最终返回空 `PoolSlot`。

返回值的容量规则：

- slab 路径返回 size class 对应的 region 大小
- buddy 路径返回 `OrderBytes(order)`

### 10.3 内部释放

`PoolSlot` 析构时调用 `ReadMemPool::ReturnSlot(off, meta)`。

`ReturnSlot` 根据 `meta` 高位的 source 识别来源：

- slab：`slab_->Free(off)`
- buddy：`FreeBuddy(off, order)`

### 10.4 外部释放

`ReleaseExternal(void* p)` 是给外部 owner 用的回收入口。

它的流程是：

1. 由地址反算 `off = p - arena_->Base()`。
2. 通过 `slab_->OwnsBlock(off)` 判断这个地址是否位于 slab block 内。
3. 若是 slab，则调用 `slab_->Free(off)`。
4. 否则用 `buddy_->OrderAt(off)` 反查 order，再调用 `FreeBuddy(off, order)`。

这条路径是当前实现里“没有 `PoolSlot` 元信息时如何归还”的关键。

## 11. 生命周期约束

当前实现有一个非常重要的生命周期前提：

`ReadMemPool::Deleter()` 返回的是一个捕获裸 `this` 的 lambda：

```cpp
return [this](void* p) { ReleaseExternal(p); };
```

这意味着：

- 任何持有这个 deleter 的外部对象，其生命周期都不能超过 `ReadMemPool`。
- 设计上必须保证 `ReadMemPool` 的生存期覆盖所有引用池内存的 `IOBuf` / `IOBuffer`。

当前代码注释明确把这个责任交给上层持有者，典型就是 mount 级长生命周期对象，例如 `VFSHub` 一类的 owner。

因此，这不是一个“普通局部对象可随手 new 出来再提前析构”的组件，而是一个需要被更高层统一托管的基础设施对象。

## 12. 并发模型

当前实现的并发控制分层如下：

| 组件 | 并发策略 |
|---|---|
| `Arena` | 构造后只读，无额外锁 |
| `BuddyAllocator` | 一个全局 `std::mutex`，保护 slow path |
| `SlabAllocator` | 一个全局 `std::mutex`，串行化所有 slab 操作 |
| `TlsCacheSet` | 每个 cache 一个 `atomic_flag` try-lock |
| `ReadMemPool` | 只负责路由，不额外加锁 |

当前路径的性能意图是：

- buddy 热路径尽量先命中 TLS cache，避免走全局 mutex
- slab 保持实现简单，当前版本用单锁换取正确性和可维护性
- buddy 和 slab 的复杂状态都不暴露给上层

## 13. 当前实现的关键不变量

下面这些是不看代码很容易遗漏、但对理解正确性非常重要的不变量：

1. `Arena::Base()` 始终 2MB 对齐。
2. `Arena::Total()` 始终是 `kMaxOrderBytes` 的整数倍。
3. buddy 里任何 handed-out block 都在同一段连续 arena VA 内。
4. `order_tag_` 中只有块起点才有有效语义。
5. slab 永远只从 buddy 申请固定 2MB block。
6. 每个 bin 最多只保留 1 个完全空闲 slab。
7. TLS cache 中缓存的 buddy block 在 buddy 看来仍算“已占用”。
8. `ReleaseExternal(void*)` 只能用于当前 pool 中分配出的有效地址。

## 14. 测试覆盖现状

当前单测位于 `test/unit/common/test_read_mem_pool.cc`，已覆盖以下核心场景：

- 创建成功、基地址非空、4K 对齐、2MB 对齐、总容量向上对齐到 64MB 倍数
- 大小不同的分配请求都得到 4K 对齐地址
- 大块与小块容量正确、互不重叠
- 耗尽后返回空 `PoolSlot`，释放后可恢复
- 大量随机 alloc/free 后，`DrainIdle()` 能恢复到 top-block 初始状态
- 4MB 连续切分后全部释放，能 coalesce 回一个 64MB top block
- `Disown()` 后对象不再自动回收，`ReleaseExternal()` 能正确释放
- 小块请求进入 slab，大块请求不进入 slab
- slab hysteresis 保留 1 个空 slab block，`DrainIdle()` 再回收
- slab 与 buddy 混合随机流量下的守恒性
- 256KB 这类中档块释放后进入 TLS cache，`DrainTls()` 后回到 buddy
- 多线程随机 alloc/free 后，`DrainIdle()` 可以回到守恒状态

从当前测试来看，这套实现已经把“正确性闭环”覆盖到以下几类核心问题：

- 对齐
- 路由
- split / coalesce
- slab block 回流
- TLS 缓存回流
- 外部释放
- 多线程守恒

## 15. 与旧文档相比的关键变化

为了避免继续按旧草案理解当前代码，这里明确列出已经变化的地方：

1. 当前实现已经拆成多文件，不再是旧文档里的 `src/common/read_mem_pool.{h,cc}` 自包含单元。
2. 返回类型已经是 `PoolSlot`，不再是旧文档中的 `PooledBuf`。
3. `Arena::Create` 当前签名是 `Create(bytes, align_to)`，由 `ReadMemPool` 传入 `kMaxOrderBytes`。
4. slab 当前固定使用 2MB block，不是“可选若干 block size”的开放设计。
5. slab 当前用单互斥锁，不是 per-bin 锁。
6. TLS cache 当前不是“每线程独占容器”，而是“固定 cache 池 + thread-local slot id”。
7. 当前代码里 oversize 请求直接失败返回空句柄，没有做自动拆分。
8. 当前文档必须把 `Deleter()` 捕获裸 `this` 的生命周期要求写成硬约束，而不能只当实现细节。

## 16. 当前边界与后续演进方向

当前实现已经完成 read-path 内存池的基础落地，但仍保持了几个明确边界：

1. 只提供 RDMA-ready 的连续地址空间，不做实际 `ibv_reg_mr`。
2. 不支持 grow/shrink，arena 在构造后固定大小。
3. `>64MB` 的请求不会自动拆分。
4. slab 当前是单锁实现，优先保证正确性和可理解性。
5. `ReleaseExternal` 依赖上层保证地址合法且生命周期正确。

如果后续要继续演进，最自然的方向是：

1. 把上层 read 真实接入 `PoolSlot` / `Disown` / `Deleter` 路径。
2. 在有明确性能数据后，再评估 slab 分桶并发优化。
3. 在生命周期模型更清晰后，再考虑把裸 `this` deleter 收敛成更强的 owner 语义。
4. 在 RDMA 路径确定后，再补 `MR` 注册和访问约束。

## 17. 结论

当前代码中的 `ReadMemPool` 已经形成了一个职责清晰的分层实现：

- `Arena` 提供连续地址空间
- `BuddyAllocator` 提供跨档流动的连续块分配
- `SlabAllocator` 优化小块内碎片
- `TlsCacheSet` 优化 buddy 中档热路径
- `PoolSlot` 提供面向调用方的 RAII 所有权语义
- `ReadMemPool` 统一对外提供分配、归还和外部释放入口

从实现状态看，这已经不是“设计草案”，而是一套已有测试闭环、语义相对稳定的基础设施代码。后续文档和接入讨论，都应以本文描述的真实代码行为为准，而不是再参考早期的 `PooledBuf` / 单文件版本草案。
