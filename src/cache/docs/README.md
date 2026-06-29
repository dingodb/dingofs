# DingoFS 缓存文档（`src/cache/docs/`）

DingoFS 缓存层（`dingo-cache` / 客户端 DiskCache / BlockCache）的设计与工程文档集。

## 目录

| 文档 | 类型 | 说明 |
|------|------|------|
| [ssd-io-characteristics.md](./ssd-io-characteristics.md) | 参考 / 备忘 | **SSD 物理特性与 IO 编程注意事项**：burst vs 稳态、队列深度、写放大、TRIM、PLP/fsync、read-after-write 同块惩罚（§1–§4）；盘越满越慢/OP、冷数据、NAND 物理延迟等级、PCIe 上限、NUMA、内核块层、FDP/ZNS（进阶 §5–§8）；含一次"读 4MiB 要 3ms"真实排查的实测数据与方法论。写缓存盘相关代码 / 做性能压测前先读。 |
| [cache-perf/](./cache-perf/) | 设计文档集 | **缓存服务端性能优化设计**（thread-per-core / RTC）：分片 LRU 锁、多 io_uring ring、消除上传回读盘、上传流水线、key→核亲和、去阻塞/Photon 等 8 篇分阶段方案。入口见 [cache-perf/README.md](./cache-perf/README.md)。 |

## 两类文档的关系

- **`ssd-io-characteristics.md` 是"硬件事实"**——盘会怎么表现、压测怎么不骗自己。它是做下面那些性能优化、以及解读任何缓存压测数字的**前置常识**。
- **`cache-perf/` 是"软件方案"**——在认清硬件与现有瓶颈后，怎么改 `dingo-cache` 的控制面去逼近硬件天花板。

> 排查/压测时记住 `ssd-io-characteristics.md` 的核心结论：**单线程 QD1 吃不满盘，"刚写完立刻读同块"会有 read-after-write 惩罚，短测/空盘的写吞吐是 burst 假象**——别拿这些数当真实容量依据。
