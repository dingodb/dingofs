# 4K 小文件读时延：定位与优化记录

日期：2026-09-03。负载：`elbencho /mnt/dingofs -r -t 1 -n 1 -N 10000 -s 4k -b 4k --direct --lat --latpercent -d`
（单线程，10000 个 4K 文件各读一次）。客户端 0003，group-1 = 0001:10001 + 0002:10002（RDMA），
两端 `--shards=8 --pin_cpu=true --cpuset=32-63`，客户端 `--cache_store=none`（纯远端命中）。
时延单位 µs；elbencho 的分位数是对数桶上界（54/64/76/91/108/128/152/181/215/256/304/362/431）。

## 结论

| 口径 | 优化前 | 优化后 | 说明 |
| :--- | :--- | :--- | :--- |
| elbencho IO latency avg | 146 | **67** | 用户口径，关访问日志 |
| elbencho IO lat p50 / p99 | ≤152 / ≤304–362 | ≤54–64 / ≤215 | |
| FUSE `read` avg / p99（开日志） | 123 / 275–296 | 57 / 181 | 客户端访问日志口径 |
| `block_store range_async` avg / p50 / p99 | 81 / 69 / 228–240 | 46 / 34 / 167 | 客户端 blockcache 提交→回调 |
| `vfs_meta read_slice` 命中 | 4.7 | 1.3 | |
| 残差（read − range_async − read_slice） | 37 | 11 | VFS/FUSE 层软件（p50 8） |

**硬件地板不是 79 µs**：这批刚写入的 4K 块文件在节点盘上 fio（QD1、direct、按文件 open/read/close）
实测 avg 13–20 / p50 10–19 / p99 68–70 / p999 70–150，数据在盘内缓冲里；79 µs 是 20 GiB 大文件随机读
的介质读时延。所以优化前软件税不是 30 µs，而是 ~125 µs（146 − 18 − 5 RDMA）。

优化后各段（bpftrace uprobe 实测，warm 元数据，p50）：

| 段 | p50 | 说明 |
| :--- | :--- | :--- |
| 内核 FUSE 往返 + libfuse | ~15 | elbencho avg 67（关日志）− FUSE read avg 57（开日志，含 3 条日志 ~5）。5.14 内核下无法去掉 |
| ClientSession → FileReader | 2 | 会话/句柄锁、指标 |
| FileReader 前处理 → ChunkReader | 5 | GetAttr、请求表、mempool 分配 |
| ChunkReader → RangeAsync | 3–5 | read_slice 命中 2.5 + ChunkReadOp |
| 客户端 blockcache 提交 → post_send | 2–6 | shard 常驻自旋后 |
| post_send → 收到应答（网络 + 节点） | 30–34 | 节点内 recv→send p50 30：盘 ~18 + 节点软件 ~12（其中 WRITE 完成等待 + SEND 5） |
| 应答 → cpu-N 回调 | 2–4 | |
| 回调 → FileReader 返回 | 2–3 | 自旋检测、拷贝引用、清理 |

尾部：`range_async` p99 ≈ 167 来自节点盘本身的 ~160 µs 一族（节点内 recv→send p99 162、p999 166，
fio 在同一批文件上 p999 150），不是软件排队。

## 根因

1. **每个请求都在唤醒睡着的 reactor**。elbencho 单线程周期 198 µs，10000 个文件按 slice id 哈希散到
   8 个客户端 shard / 每节点 8 个 shard，每个 shard 每 1.6 ms（客户端）/ 3.2 ms（节点）才来一个请求，
   远大于 `idle_poll_us=200`；客户端 shard（eventfd + io_uring 唤醒 12–30 µs）、客户端 `cpu-N`
   （condvar 4 µs）、节点 shard（CQ 事件通道 + io_uring ~10 µs）每次都先睡后醒。cb 压测同 chunk 同
   shard 周期 66 µs 从未暴露。
2. **前台读走异步线程池 + condvar 唤醒**。小文件整读必进"最后 32KB 预读"分支，`MakeReadahead`
   把请求投给 `vfs_read` 线程池（mutex+condvar，唤醒 5–10 µs），完成后再 `cv.notify_all` 唤醒 fuse
   线程（5–10 µs）。
3. **热路径杂税**：trace 关闭时每层仍 `make_shared<Context>`（~14 次/读）；`ReadMemPool::OutstandingBytes`
   走 `bvar::Adder::get_value()`（遍历全部线程）每读 3 次；`vfs_meta` 访问日志一行 2–3 µs。
4. **冷元数据**：首次读 `read_slice` 一次 MDS 往返 115–138 µs；`ReadChunkCache` 只新鲜 10 s；目录预热
   触发后每次 open 都重复提交任务，队列满后每次 open 一条 `LOG(ERROR)`（open avg 19→26、p99 38→82）。

## 改动

参数（客户端挂载与节点 `node.conf`）：

```
# 节点
--idle_poll_us=5000            # 请求后 5 ms 内不睡，空闲仍睡；poll_mode=true 是 8 核常年 100% 的对照
# 客户端
--idle_poll_us=5000 --offload_cpu_spin_us=5000
--vfs_read_spin_wait_us=200    # 新增：fuse 线程自旋等完成，超时才 condvar
--vfs_meta_chunk_fresh_time_s=600
```

代码：

- `FileReader::Read`：请求已覆盖"最后 32KB"区间时不再预读；单个前台请求在调用线程内联 `DoReadRequst`
  （多请求仍走线程池以保证并发发出）；等待前按 `vfs_read_spin_wait_us` 自旋，`ReadRequest::state`
  改为 atomic 供无锁轮询。
- blockcache `CpuWorker` 绑到所在 shard 核的 SMT 兄弟核（`SmtSiblingOf`），完成握手不跨 socket。
- 读路径各层用已有的 `SpanScope::GetContext(span, ctx)` 透传上下文；`TraceManager::Start*Span`
  改 `string_view`，trace 关闭零构造。
- `ReadMemPool::outstanding_bytes_` 改 `std::atomic`。
- 预热：`AsyncWarmupSmallFile` 先查 `WarmupMemo`，提交失败降为 `LOG_EVERY_N(WARNING)`。
- `range_async` 访问日志只计到回调前，日志在回调之后写。

## 各步收益（warm 元数据，开日志口径，avg）

| 步骤 | elbencho avg | FUSE read | range_async | 残差 |
| :--- | :--- | :--- | :--- | :--- |
| 基线 | 146 | 123 | 81 | 37 |
| + 节点 idle_poll_us=5000 | 141 | 117 | 75 | 37 |
| + 客户端 idle_poll_us/offload_cpu_spin_us=5000 | 99 | 81 | 46 | 31 |
| + 代码（内联/自旋/透传/绑核） | 85 | 68 | 46 | 19 |
| + trace 名字 string_view、内联仅单请求 | 72 | 57 | 45 | 11 |
| 关访问日志（用户口径） | **67** | – | – | – |

## 方法

- 三层访问日志顺序对齐（单线程严格顺序）出残差；`bpftrace` uprobe 切分客户端各段；节点用
  `perf probe --no-demangle` 打 `OnMessageReceived`/`OpenReadCloseAwaiter::await_resume`/`RDMASend` 三个探针。
- 节点盘地板：把这批块文件硬链接到一个目录，`fio --opendir --file_service_type=random --openfiles=1`。
- 节点重启后 `DiskCacheLoader` 会扫全部块文件（17 万个），期间读时延翻倍，冷读数据要等扫描结束。
- 节点进程是 root 起的，重启/重挂都要 `sudo`；挂载点也是 root 的，`fusermount3 -u` 要 `sudo`。

## 后续

- 内核 FUSE 往返 ~17 µs：需要 FUSE over io_uring（6.14+）或 passthrough，本内核不可用。
- 节点 WRITE 完成等待 + SEND ~5 µs：应答改同 QP 链式 / WRITE_WITH_IMM 可省。
- 节点 openat 路径查找：常驻 fd 表 / `open_by_handle_at`。
- 首次读的 MDS 往返：lookup/open 应答附带小文件 slice。

## 32 并发（elbencho -t 32）为什么看起来慢一倍

`elbencho /mnt/dingofs -r -t 32 -n 1 -N 10000 -s 4k -b 4k --direct --lat -d`，各层随并发扫描
（同一挂载，暖元数据，开日志口径，µs）：

| 线程 | range_async avg / p50 / p99 | 残差 avg | 备注 |
| :--- | :--- | :--- | :--- |
| 1 | 55 / 35 / 174 | 15 | 盘命中盘内 DRAM 缓冲 |
| 4 | 91 / 87 / 172 | 13 | 盘开始脱离缓冲 |
| 8 | 99 / 108 / 173 | 16 | |
| 16 | 102 / 109 / 174 | 20 | range_async 已平台 |
| 32 | ~110 / 110 / ~230 | 40–140（抖） | 残差随主机负载抖 |

**结论：读缓存本身没有随并发线性变慢。** `range_async`（客户端提交→RDMA→节点盘→回调）从 t=4 起就
平台在 p50 ~110、p99 ~174，t=16 与 t=32 几乎一样。看起来"比单线程翻倍"是因为：

- **单线程是被 SSD 盘内缓冲喂出来的假地板。** t=1 只读 r0 共 1 万文件（每块缓存盘约 5 MB），全部命中盘
  的 DRAM 缓冲 → 盘 avg ~18µs（fio 实测同批文件 QD1~QD16 都是 ~20µs，因为反复读同 4000 文件全在缓冲）。
- **并发后工作集撑大到脱离缓冲。** t=32 读 r0–r31 共 32 万个不同文件（每盘约 230 MB），超出盘 DRAM 缓冲
  → 变成介质随机读 ~78µs（正是 perftest 文档给 4K 随机点查预算的 78µs 地板）。
- **拆账（节点 io_uring 块读 submit→complete 实测，按 req 指针精确配对）**：
  盘读 p50 t=1 **22µs** → t=32 **97µs**（+75），而 `range_async` p50 35→110（+75）完全对应。
  即 t=1 = 盘 22 + RDMA 5 + 软件 ~8；t=32 = 盘 97 + RDMA 5 + 软件 ~8。**软件税全程 ~8µs 不变，一点没涨**，
  全部增量都是节点盘那一段。（p99 两档都是 ~160µs，是介质读的尾；t=1 大多命中盘缓冲、t=32 大多走介质，双峰。）
- 盘读这段覆盖链式 `openat_direct → read_fixed → close_direct`（read 用 IO_LINK 挂在 open 后，必须等 open
  完成才开始），所以 = openat(XFS 路径查找 + 冷 inode 读) + NAND 读。t=1 只读 r0 一万文件、dentry/inode 全热 +
  数据在盘内写缓冲 → 22µs；t=32 读 32 万个不同小文件、dentry/inode 冷 + 超出盘缓冲 → 冷 openat + 介质随机读 → 97µs。

**t=32 真正会恶化的是客户端残差（VFS/FUSE 层）15→40–140µs，且抖动大**，根因是**压测主机 CPU 争用**：
这台机器同机跑着整套存储集群（dingodb_server 峰值 2000%+ CPU、tikv、mds）+ 32 个 elbencho 线程 +
~57 个 fuse worker + 8 shard + 8 cpu-N（后两者钉在 node1），全挤在 node1 的 32 个物理核上，负载常年
30–60。dingodb 一抽风残差就冲到 140、p99 冲到 4–5 ms。把 elbencho 钉到 node0 或换空载机器，残差回到
30–40µs。这不是客户端代码问题（`vfs_read_spin_wait_us` 取 0 vs 200 的 t=32 残差是 128 vs 110，自旋反而
略好），也不是本轮改动引入的回退。

### 本轮为并发稳健性做的改动

- `vfs_read_spin_max_waiters=4`（新）：同时自旋等待的前台读最多 4 个，宽扇出（32 并发）时其余直接 park，
  单线程的自旋优化不会在高并发把核烧光。
- 预热触发去重：`AsyncWarmupSmallFile` 先查 `WarmupMemo`，8 s 内已预热则跳过；提交失败 `LOG_EVERY_N`，
  消除每次 open 一条 `submit warmup task fail` 的 ERROR 刷屏。
- `vfs_meta_warmup_small_file_data_enable`（新，默认 true）：**远端纯缓存（cache_store=none）建议设 false**。
  数据预热把小文件块拉进本地盘缓存，无本地盘时它只会产生海量 `put_block`/`prefetch_async`（实测旧客户端
  预热阶段 63 万次 put + 34 万次 prefetch，把盘和 MDS 打满），对 t=32 读毫无收益。设 false 后 t=32 只剩
  纯 `range_async`，无任何后台块流量。
- 无会话 `ReadSlice`（预热/prefetch 调用）先查 `ReadChunkCache`，命中就不发 MDS；chunk 预热改为同步先行、
  data 预热在后，让 data 预热的逐文件 slice 查询命中本地缓存。

### 32 并发推荐挂载参数（远端纯缓存）

在单线程那组基础上加：`--vfs_meta_warmup_small_file_data_enable=false`。其余不变。
