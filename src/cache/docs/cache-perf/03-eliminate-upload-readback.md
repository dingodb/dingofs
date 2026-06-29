# 消除 Writeback 上传时的"回读盘" IO 放大

## 概述

DingoFS 缓存节点（CacheNode）的 writeback 写入链路存在一处明显的 IO 放大：客户端
通过 `Put` 把一个 block 写到缓存盘的 stage 文件之后，后台的 `BlockCacheUploader`
在把该 block 上传到对象存储之前，会先把**刚刚落盘的同一个 block 从磁盘完整读回内存**
（`store_->Load(...)`），然后再 `storage_client->Put(...)`。

也就是说，每一个 writeback block 在它的生命周期里要经历"写一次盘 + 读一次盘 + 传一次
对象存储"。其中的"读一次盘"完全是多余的——因为该 block 的内存 buffer 在 Stage 时本
就在内存里（来自网络请求 attachment，引用计数零拷贝），只是上传任务没有把它带过来。
按默认 4 MiB block、典型写带宽估算，这一步给每块缓存盘平白增加了与写入等量的随机/顺序
读 IO，挤占了真正的读缓存命中带宽，并提高了上传延迟（必须等回读完成才能发起 PUT）。

本设计的目标：**Stage 时把 block 的内存 buffer 保留，并随上传任务一路传递到
`DoUpload`，使上传直接复用内存数据，免去回读盘**；仅在 buffer 已被释放/淘汰、或属于
重启 reload 路径时，才回退到原来的 `Load` 读盘逻辑。

## 背景与动机

### 写入链路（从网络到落盘再到上传）

1. `src/cache/node/service.cc:107` `BlockCacheServiceImpl::Put`
   - `src/cache/node/service.cc:116` `IOBuffer block = GetRequestAttachment(controller);`
     —— block 数据来自 brpc/RDMA 的 request attachment（`request_attachment().movable()`，
     见 `service.cc:80-92`），是引用计数的 `IOBuffer`，零拷贝，**全程在内存里**。
   - `src/cache/node/service.cc:117` `node_->Put(std::move(handle), std::move(block));`
2. `src/cache/node/node.cc:185` `CacheNode::Put`
   - `src/cache/node/node.cc:190` `block_cache_->Put(..., {.writeback = true});`
3. `src/cache/local/local_block_cache.cc:146` `LocalBlockCache::Put`
   - `src/cache/local/local_block_cache.cc:151` `store_->Stage(handle, std::move(block), {.block_attr = ...});`
4. `src/cache/local/disk_cache.cc:185` `DiskCache::Stage`
   - `src/cache/local/disk_cache.cc:201` `localfs_->WriteFile(stage_path, &block);` —— 落 stage 文件
   - `src/cache/local/disk_cache.cc:211` `iutil::Link(stage_path, cache_path);` —— 硬链接，使 block 同时可被读缓存命中
   - `src/cache/local/disk_cache.cc:220` `manager_->Add(handle, CacheValue(length, ...), BlockPhase::kStaging);`
   - `src/cache/local/disk_cache.cc:223` `uploader_(handle, length, option.block_attr);`
     —— 注意：此处 `uploader_` 是 `CacheStore::UploadFunc`，**只携带
     `handle / length / block_attr`，没有携带 block 的内存 buffer**；函数返回后
     `block` 析构，内存随之释放。

`UploadFunc` 定义（`src/cache/local/cache_store.h:60`）：

```cpp
using UploadFunc = std::function<void(BlockHandle handle, size_t length,
                                      BlockAttr block_attr)>;
```

该回调由 `LocalBlockCache::Start` 注入（`src/cache/local/local_block_cache.cc:114-117`）：

```cpp
auto status = store_->Start(
    [this](BlockHandle handle, size_t length, BlockAttr block_attr) {
      uploader_->EnterUploadQueue(StageBlock(handle, length, block_attr));
    });
```

`StageBlock`（`src/cache/local/block_cache_uploader.h:39-46`）当前只含
`handle / length / block_attr`，没有 buffer 字段。

### 回读盘发生在上传任务里

`src/cache/local/block_cache_uploader.cc:262` `BlockCacheUploader::DoUpload`：

```cpp
Status BlockCacheUploader::DoUpload(const StageBlock& sblock) {
  IOBuffer buffer;
  auto status = store_->Load(sblock.handle, 0, sblock.length, &buffer);  // :264 回读盘
  ...
  status = storage_client->Put(sblock.handle, buffer);                   // :282 上传
  ...
  status = store_->RemoveStage(sblock.handle, ...);                      // :289 删 stage 文件
  return status;
}
```

`store_->Load` 最终落到 `DiskCache::Load`（`src/cache/local/disk_cache.cc:273`）→
`localfs_->ReadFile(cache_path, ...)`（`src/cache/local/disk_cache.cc:292`），即对
cache 路径发起一次 `O_DIRECT` AIO 读，把整块 4 MiB 读回内存。

### 为什么这次读盘是多余的

落盘路径 `LocalFileSystem::WriteFile`（`src/cache/local/local_filesystem.cc:175`）接收
的是 `const IOBuffer*`，它要么直接拿该 buffer 的固定 slab 做 AIO 写
（`local_filesystem.cc:226-227`），要么把它 `CopyTo` 到一块临时固定 slab 再写
（`local_filesystem.cc:228-235`）。**无论哪条路，原始 block 的 backing 内存在
`WriteFile` 返回后都仍然有效**——`WriteFile` 只读不毁。换言之：在 `Stage` 返回那一刻，
我们手里就握着一份和盘上内容完全一致的内存 buffer，却把它丢弃，几毫秒后又从盘上读回来。

证据链小结（均为真实代码）：

| 关注点 | 位置 |
| --- | --- |
| block 来自网络 attachment（内存、引用计数） | `src/cache/node/service.cc:80-92,116` |
| `Stage` 落盘后丢弃 buffer，仅回调 handle/length/attr | `src/cache/local/disk_cache.cc:201,223` |
| `UploadFunc` 签名不含 buffer | `src/cache/local/cache_store.h:60` |
| `StageBlock` 不含 buffer | `src/cache/local/block_cache_uploader.h:39-46` |
| 上传前回读盘 | `src/cache/local/block_cache_uploader.cc:264` |
| `WriteFile` 只读不毁 buffer | `src/cache/local/local_filesystem.cc:175,226-235` |

## 当前实现分析

### 数据流（现状）

```
Client ──PUT(attachment)──► BlockCacheServiceImpl::Put
                                 │  IOBuffer block  (内存, refcount)
                                 ▼
                            CacheNode::Put ─► LocalBlockCache::Put ─► DiskCache::Stage
                                                                          │
                                       ┌──────────────────────────────────┤
                                       │ WriteFile(stage_path,&block)  ①写盘
                                       │ Link(stage→cache)             ②硬链接
                                       │ manager_->Add(kStaging)
                                       │ uploader_(handle,length,attr) ──┐  block 析构，内存释放
                                       └─────────────────────────────────┘
                                                                          │ (只传 handle/length/attr)
                                                                          ▼
                                                              BlockCacheUploader 队列
                                                                          │
                                                                          ▼
                                                              DoUpload(sblock)
                                                                  store_->Load(...)  ③回读盘  ◄── 放大点
                                                                  storage_client->Put ④上传对象存储
                                                                  RemoveStage         ⑤删 stage 文件
```

每块 writeback block 的盘 IO：① 写 + ③ 读，读是纯放大。

### 上传队列与并发模型

- `BlockCacheUploader::UploadWorker`（`block_cache_uploader.cc:190`）单线程从优先级队列
  `PendingQueue` 取任务（writeback 优先于 reload），对每个 `StageBlock` 调用
  `AsyncUpload`。
- `AsyncUpload`（`block_cache_uploader.cc:208`）用 `InflightTracker` 去重并限流
  （`FLAGS_upload_stage_max_inflights`，默认 32，见 `block_cache_uploader.cc:40-42`），
  在 bthread 里跑 `DoUpload`，完成后 `OnComplete` 决定是否重试。
- 失败重试：`OnComplete`（`block_cache_uploader.cc:236`）对一般 error 会
  `bthread_usleep` 后 `EnterUploadQueue(sblock)` 重新入队。
- reload 路径：`DiskCacheLoader::RegisterBlock`（`disk_cache_loader.cc:184-200`）扫盘发现
  残留的 stage 文件后，以 `BlockAttr::kFromReload` 调用 `uploader_`，**此时进程内没有该
  block 的内存 buffer**，只能读盘。

### slab/内存现状

- 全局两个 slab 池：read / write，各 `FLAGS_iodepth` 个 4 MiB buffer，lock-free
  （`src/cache/common/slab_buffer.cc:122-136`、`src/cache/common/slab_buffer.h`）。
- writeback 的 block 数据并不必然占用 slab：它来自网络 attachment，是 brpc 自己的
  `IOBuf` block（非 slab）。`WriteFile` 在该 buffer 不是固定 slab 时会临时借一块 write
  slab 做对齐拷贝，并在 `WriteFile` 返回时立刻归还（`AppendUserDataWithMeta` 的 deleter
  在 `fixed` 析构时 `pool->Free`）。因此**保留 block 内存 ≠ 一定占住 slab**，多数情况下
  保留的是 brpc 的引用计数 IOBuf 内存，不消耗 slab 池容量。

## 设计方案

### 核心思想

把 Stage 时已在内存中的 block buffer **作为上传任务的一部分保留并传递**，让 `DoUpload`
优先使用内存 buffer 上传；只有当内存 buffer 不可用（reload 扫盘任务、或保留失败/被主动
释放）时，才回退到 `store_->Load` 读盘。

### 关键改动一：`UploadFunc` 与 `StageBlock` 携带 buffer

`UploadFunc` 增加一个可选的 `IOBuffer`：

```cpp
// src/cache/local/cache_store.h
using UploadFunc = std::function<void(BlockHandle handle, size_t length,
                                      BlockAttr block_attr,
                                      std::shared_ptr<IOBuffer> buffer)>;
//                                      ^ writeback: 非空; reload: nullptr
```

`StageBlock` 增加 buffer 字段（用 `shared_ptr<IOBuffer>` 以便在重试/多处持有时安全共享
所有权，nullptr 表示"无内存数据，需读盘"）：

```cpp
// src/cache/local/block_cache_uploader.h
struct StageBlock {
  StageBlock(BlockHandle handle, size_t length, BlockAttr block_attr,
             std::shared_ptr<IOBuffer> buffer = nullptr)
      : handle(std::move(handle)), length(length),
        block_attr(block_attr), buffer(std::move(buffer)) {}

  BlockHandle handle;
  size_t length;
  BlockAttr block_attr;
  std::shared_ptr<IOBuffer> buffer;  // 非空: 内存直传; 空: 回读盘
};
```

> 选用 `shared_ptr<IOBuffer>` 而非按值 `IOBuffer`：`StageBlock` 在 `PendingQueue`、
> `Segments`、`AsyncUpload` 的 bthread lambda（按值捕获 `sblock`）、`OnComplete` 重试
> 入队等多处被拷贝；`IOBuffer` 本身基于 brpc `IOBuf` 已是浅拷贝/引用计数，但用
> `shared_ptr` 包一层能让"buffer 是否仍存活"的语义显式化，并保证只在最后一个持有者析构
> 时才释放底层内存。

### 关键改动二：Stage 把落盘成功的 buffer 交给 uploader

`DiskCache::Stage` 在 `WriteFile` 成功后，把 `block` 包进 `shared_ptr` 随回调传出。注意
`Stage` 的入参 `block` 是按值传入的 `IOBuffer`（`disk_cache.cc:185`），落盘只用了它的
`const` 引用，可以安全地把它的所有权转移给上传任务：

```cpp
// src/cache/local/disk_cache.cc  Stage()
status = localfs_->WriteFile(stage_path, &block);
if (!status.ok()) { ... return status; }

iutil::Link(stage_path, cache_path);   // 不变
manager_->Add(handle, CacheValue(length, iutil::TimeNow()), BlockPhase::kStaging);

// 仅 writeback 路径保留 buffer；其它来源（理论上 Stage 仅 writeback）保持空
auto kept = std::make_shared<IOBuffer>(std::move(block));
uploader_(handle, length, option.block_attr, std::move(kept));
return status;
```

回调闭包（`local_block_cache.cc:114-117`）相应改为：

```cpp
auto status = store_->Start(
    [this](BlockHandle handle, size_t length, BlockAttr block_attr,
           std::shared_ptr<IOBuffer> buffer) {
      uploader_->EnterUploadQueue(
          StageBlock(handle, length, block_attr, std::move(buffer)));
    });
```

### 关键改动三：`DoUpload` 优先内存直传，失败回退读盘

```cpp
// src/cache/local/block_cache_uploader.cc  DoUpload()
Status BlockCacheUploader::DoUpload(const StageBlock& sblock) {
  IOBuffer buffer;
  if (sblock.buffer != nullptr && sblock.buffer->Size() == sblock.length) {
    buffer = *sblock.buffer;        // 内存直传，零拷贝（IOBuf 引用计数）
  } else {
    auto status = store_->Load(sblock.handle, 0, sblock.length, &buffer);  // 回退读盘
    if (status.IsNotFound()) {
      LOG(ERROR) << "Fail to upload " << sblock << " which already deleted";
      return status;
    } else if (!status.ok()) {
      return status;
    }
  }

  StorageClient* storage_client;
  auto status = storage_client_pool_->GetStorageClient(sblock.handle.FsId(),
                                                       &storage_client);
  if (!status.ok()) { return status; }

  status = storage_client->Put(sblock.handle, buffer);
  if (!status.ok()) { return status; }

  status = store_->RemoveStage(sblock.handle, {.block_attr = sblock.block_attr});
  if (!status.ok()) { status = Status::OK(); }  // 忽略 RemoveStage 错误
  return status;
}
```

要点：
- `buffer = *sblock.buffer` 是 `IOBuffer` 的拷贝赋值，底层 brpc `IOBuf` 共享 block，
  **零数据拷贝**；`storage_client->Put` 内部按 iovec 发送，不要求单块连续。
- `reload` 任务的 `sblock.buffer == nullptr`，自然走读盘分支，行为与现状一致。
- 增加 `Size() == length` 校验，防御性地避免"buffer 与落盘长度不一致"的边界（理论上不会
  发生，但作为安全网；不一致则回退读盘）。

### 数据流（改造后）

```
Client ──PUT(attachment)──► ... ─► DiskCache::Stage
                                       │ WriteFile(stage_path,&block)  ①写盘
                                       │ Link / manager_->Add(kStaging)
                                       │ kept = shared_ptr<IOBuffer>(move(block))
                                       │ uploader_(handle,length,attr, kept) ──┐
                                       └────────────────────────────────────────┘
                                                                          │ (携带内存 buffer)
                                                                          ▼
                                                              StageBlock{..., buffer=kept}
                                                                          ▼
                                                              DoUpload(sblock)
                                                                  buffer = *sblock.buffer  (内存直传, 无 ③读盘)
                                                                  storage_client->Put       ④上传
                                                                  RemoveStage               ⑤删 stage 文件
                                                                  ▲ 内存 buffer 在此随 sblock 析构而释放
```

reload 路径不变：

```
DiskCacheLoader 扫盘 ─► RegisterBlock ─► uploader_(handle,size,kFromReload, nullptr)
                                                 ▼
                                       StageBlock{buffer=nullptr} ─► DoUpload ─► store_->Load(读盘) ─► Put
```

### 内存压力与生命周期管理

这是本方案最需要谨慎的部分：保留 in-flight buffer 会延长内存占用时间。

1. **占用量上界是有界且可控的**。上传并发被 `FLAGS_upload_stage_max_inflights`（默认 32）
   限流——但要注意：`PendingQueue` 里**排队等待**的 `StageBlock` 也会持有 buffer，队列长度
   取决于上传是否跟得上写入。因此真实上界是"pending 队列长度 × block 大小"。
   - 对策：保留 buffer 的总字节数受 stage 容量天然约束——`DiskCache::Stage` 入口已有
     `CheckStatus(kWantStage)`，stage 盘满（`StageFull`）即拒绝新 Stage
     （`disk_cache.cc:191`、`disk_cache_manager` 的 `stage_full`）。但盘容量（百 GB 级）
     远大于内存，不能直接当内存上界。
   - **新增内存水位限流**：为"已保留但尚未上传完成"的 buffer 设一个独立的内存配额
     `FLAGS_upload_keep_buffer_max_bytes`（默认建议 `upload_stage_max_inflights *
     4 MiB * k`，例如 256 MiB）。在 `EnterUploadQueue` 入队前用一个原子计数器累加
     `length`；超过配额时**不保留 buffer（置 nullptr，回退读盘）**，从而把内存占用钳在
     上界内，同时不丢失正确性（读盘是安全回退）。上传完成的 `OnComplete` 里递减计数器。
     这样在写入远快于上传的反压场景下自动退化为"老行为"，避免 OOM。

2. **slab 池不会被上传长期占住**。如前分析，writeback buffer 来自 brpc attachment 而非
   slab；`WriteFile` 借用的 write slab 在 `WriteFile` 内即归还。保留的是引用计数的 brpc
   IOBuf 内存，不影响 read/write slab 池的可用 buffer 数，因此**不会因为保留上传 buffer
   而饿死并发的读缓存（`AllocSlabBuffer`/`RetrievePartBlock` 等）或后续写入的固定缓冲**。
   （若未来某条写入路径改为直接把数据放进 write slab 再 Stage，则需把这些 slab buffer
   计入上面的内存配额，见"风险与对策"。）

3. **IOBuf 引用计数所有权**。`shared_ptr<IOBuffer>` 保证：
   - 入队、bthread 按值捕获、重试再入队等多处持有期间，底层 IOBuf block 不被释放；
   - 直到上传成功（或彻底放弃）后 `StageBlock` 析构，最后一个 `shared_ptr` 归零，内存
     才释放。这天然覆盖了**失败重试**：`OnComplete` 重试时 `EnterUploadQueue(sblock)`
     拷贝的是同一个 `shared_ptr`，buffer 在整个重试链路里一直存活，重试时仍走内存直传，
     不会因为重试而退回读盘。

4. **reload（重启扫盘）路径仍需读盘**。重启后进程内没有 buffer，`RegisterBlock` 传
   `nullptr`，`DoUpload` 自然走 `Load`。这是不可避免且正确的——内存数据已随上次进程退出
   而消失。

### 失败与边界场景的处理矩阵

| 场景 | buffer 是否存活 | DoUpload 行为 |
| --- | --- | --- |
| 正常 writeback，首次上传 | 是 | 内存直传 |
| writeback 上传失败重试 | 是（shared_ptr 贯穿重试） | 内存直传 |
| 内存配额超限时入队 | 否（主动置 nullptr） | 读盘回退 |
| reload 扫盘任务 | 否（nullptr） | 读盘回退 |
| `CacheDown` 失败（`OnComplete` 不重试，等重启） | 进程退出后失效 | 重启后由 reload 读盘 |
| buffer 与落盘长度不一致（防御） | 是但被判定不可信 | 读盘回退 |

## 文件结构

涉及改动的文件（均为既有文件，无新增文件）：

```
src/cache/local/cache_store.h               # UploadFunc 签名增加 buffer 形参
src/cache/local/block_cache_uploader.h      # StageBlock 增加 buffer 字段; 内存配额计数器声明
src/cache/local/block_cache_uploader.cc     # DoUpload 内存直传/回退; EnterUploadQueue 配额限流; OnComplete 递减计数
src/cache/local/disk_cache.cc               # Stage 保留 block 并随回调传出
src/cache/local/local_block_cache.cc        # store_->Start 注入的回调适配新签名
src/cache/local/disk_cache_loader.cc        # RegisterBlock 调 uploader_ 时传 nullptr (reload)
src/cache/local/mem_cache.cc                # MemCache::Start/Stage 的 uploader_ 调用适配新签名
```

> `mem_cache.cc:94` 也调用了 `uploader_(handle, length, option.block_attr)`，需要一并
> 适配签名。MemCache 本就把数据放内存，可顺带把 buffer 传出（或简单传 nullptr 保持现状，
> 视 MemCache 是否参与上传而定——保守起见先传 nullptr）。

## 实现步骤

1. **改签名（编译期可验证的最小步）**
   - `cache_store.h`：`UploadFunc` 增加 `std::shared_ptr<IOBuffer> buffer` 形参。
   - `block_cache_uploader.h`：`StageBlock` 增加 `std::shared_ptr<IOBuffer> buffer`
     字段及构造参数（默认 nullptr）。
   - 修正所有 `uploader_(...)` / `EnterUploadQueue(StageBlock(...))` 调用点
     （`disk_cache.cc:223`、`disk_cache_loader.cc:191`、`mem_cache.cc:94`、
     `local_block_cache.cc:116`），先全部传 nullptr，保证编译通过、行为与现状一致。

2. **Stage 保留 buffer**
   - `disk_cache.cc:Stage`：`WriteFile` 成功后 `make_shared<IOBuffer>(std::move(block))`
     并传给 `uploader_`。注意确保在 `WriteFile` 之后再 move（之前 `WriteFile` 仍需读
     `block`）。

3. **DoUpload 内存直传 + 回退**
   - `block_cache_uploader.cc:DoUpload`：按"设计方案-关键改动三"改写，保留
     `Load`/`Put`/`RemoveStage` 的错误处理语义不变。

4. **内存水位限流**
   - 新增 `DEFINE_uint64(upload_keep_buffer_max_bytes, 256 << 20, ...)`。
   - `BlockCacheUploader` 增加 `std::atomic<uint64_t> keep_buffer_bytes_{0}`。
   - `EnterUploadQueue`：若 `sblock.buffer != nullptr`，先 `fetch_add(length)`；若超过
     配额则回滚（`fetch_sub`）并把 `sblock.buffer` 置空（退化为读盘）。
   - `OnComplete`（成功/放弃路径）与 `DoUpload` 完成后：对实际携带了 buffer 的任务
     `fetch_sub(length)`。需保证"加"与"减"配对一次（建议在 `AsyncUpload` 完成回调里统一
     减，避免重试时重复减）。

5. **reload 路径确认**
   - `disk_cache_loader.cc:RegisterBlock` 显式传 `nullptr`，并加注释说明"重启后无内存
     数据，必须读盘"。

6. **观测**
   - 新增 bvar：`upload_from_memory_count` / `upload_from_disk_count`、
     `upload_keep_buffer_bytes`（当前保留字节数），用于灰度时量化命中率与内存占用。
   - 复用现有 `DiskCacheMetricsGuard`/`StageBlockStat`，必要时扩展。

7. **回归与压测**（见测试方案）。

## 兼容性与灰度

- **磁盘格式无变化**：stage 文件、硬链接、目录布局完全不变；reload 逻辑不变。新旧版本可
  互相读取对方写下的 stage/cache 文件，可平滑升级/回滚。
- **对象存储语义不变**：上传的 PUT 内容字节与现状一致（内存 buffer 与落盘内容同源），
  只是数据来源从"盘"变为"内存"。
- **接口变更仅限进程内**：`UploadFunc`/`StageBlock` 是 cache 模块内部接口，不涉及 RPC
  协议、不涉及跨节点兼容。
- **灰度开关**：建议加 `DEFINE_bool(upload_from_memory, true, ...)`。置 `false` 时
  `Stage` 始终传 nullptr，完全退回老的"回读盘"行为，作为线上快速回退手段。配合
  `upload_keep_buffer_max_bytes` 可分级灰度（先小配额观察内存，再放大）。
- **MemCache** 暂保持传 nullptr（行为不变），后续可单独评估。

## 风险与对策

| 风险 | 说明 | 对策 |
| --- | --- | --- |
| 内存占用上涨 / OOM | 上传慢于写入时 pending 队列堆积，保留的 buffer 累积 | `upload_keep_buffer_max_bytes` 硬配额，超限自动退化为读盘；新增 `upload_keep_buffer_bytes` bvar 告警 |
| slab 池被挤占 | 若 buffer 实际占用 write/read slab，长 in-flight 会饿死读/写 | 当前 writeback buffer 非 slab（来自 brpc attachment），不占池；若未来写入路径改用 slab，须把这些 slab 计入内存配额，且配额需小于 `iodepth` 总量 |
| 内存数据与盘内容不一致 | 极端 bug 导致 buffer 与落盘字节不同 | `DoUpload` 内存直传前校验 `buffer->Size()==length`；不一致则回退读盘；上传内容始终与盘同源（同一 `block`） |
| 重试期间 buffer 提前释放 | 失败重试再入队若拷贝丢了所有权会变成 nullptr | 用 `shared_ptr<IOBuffer>`，重试链路全程共享同一所有权，buffer 存活到最终成功/放弃 |
| 配额计数器加减不配对 | 重试/异常路径漏减导致配额泄漏，最终全部退化读盘 | 计数加在 `EnterUploadQueue`、减在 `AsyncUpload` 的 bthread 完成回调（每个任务恰好一次），重试视为新任务重新加减，并加 DCHECK/bvar 监控 |
| reload 误用内存直传 | reload 任务若误带 buffer 会读到错误内存 | `RegisterBlock` 强制传 nullptr；`DoUpload` 对 `from==kFromReload` 可加断言 buffer 为空 |
| 上传成功但 RemoveStage 失败 | 行为与现状一致（忽略错误） | 不变；buffer 在 `DoUpload` 返回后随 `StageBlock` 析构释放 |

## 测试方案

### 单元测试

1. **内存直传命中**（核心）：构造 `DiskCache::Stage`，mock `StorageClient::Put` 校验其
   收到的 buffer 字节与传入 block 一致；断言 `DoUpload` 期间**没有发生 `store_->Load`**
   （可通过给 `DiskCache::Load` 计数或 mock `CacheStore`）。验证 `upload_from_memory_count`
   递增、`upload_from_disk_count` 不变。
2. **reload 回退读盘**：直接以 `StageBlock{buffer=nullptr, from=kFromReload}` 入队，断言
   走 `Load`，`upload_from_disk_count` 递增。
3. **失败重试保留 buffer**：mock `Put` 首次返回可重试 error、第二次成功；断言两次都走
   内存直传（`Load` 调用 0 次），且最终 `RemoveStage` 被调用。
4. **配额限流退化**：把 `upload_keep_buffer_max_bytes` 设得很小，连续 Stage 多块，断言
   超额的块 buffer 被置空、走读盘，计数器加减最终归零无泄漏。
5. **长度不一致防御**：构造 buffer.Size != length，断言回退读盘。
6. **签名适配回归**：`mem_cache`/`disk_cache_loader` 调用点编译与行为回归。

涉及测试目录：`test/cache/local/`（参照现有 `block_cache_uploader` / `disk_cache`
相关单测组织方式新增/扩展用例）。

### 集成 / 压力测试

1. **IO 放大验证**：开启 writeback，持续写入 N 个 4 MiB block，统计缓存盘的读 IO 量
   （`iostat`/盘 bvar）。改造前应观察到约等于写入量的额外读；改造后 reload=0 场景下盘读
   IO 应趋近 0（仅元数据/校验类），写带宽与上传带宽不变或更优。
2. **上传延迟**：对比改造前后 `Stage→Put` 的端到端上传延迟分布，预期 p50/p99 下降
   （省去一次 4 MiB 读盘）。
3. **内存反压**：制造"写远快于上传"（限速对象存储），观察 `upload_keep_buffer_bytes` 稳定
   钳在配额内、进程 RSS 不失控、超额块正确退化为读盘且上传不丢块。
4. **重启 reload 正确性**：写入若干 block 后在上传完成前 kill 进程，重启，断言扫盘
   reload 把残留 stage 全部读盘上传成功、对象存储内容正确、stage 文件被清理。
5. **回退开关**：`upload_from_memory=false` 时行为与老版本逐字节一致（回归基线）。

### 验证命令（参考）

```bash
# 构建 cache 相关 target 后，运行相关单测
./test.py --mode dev --name block_cache_uploader
./test.py --mode dev --name disk_cache
```
