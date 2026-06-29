# 02-queue-depth — 队列深度:QD1 吃不满 NVMe

> 一句话:一次只发一个同步 IO(QD1)填不满 NVMe 的内部并行度,要靠很多 IO 同时在飞。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: QD16 vs QD1 -> 1.59x [EFFECT CONFIRMED]。QD1 只有 4.46 GB/s,QD4 起就顶到 ~7 GB/s 的 PCIe Gen4 x4 链路天花板,再加深度不再涨。

## 这个 demo 测什么
读一个 4GiB 文件,O_DIRECT bs=4MiB,用线程数当"在飞的 IO 深度":threads=1(≈QD1)、4、16,看聚合带宽。

## 机制
NAND 内部有几十个 die/channel,天生并行;一次一个同步 IO 让大部分并行度闲着。~7 GB/s 是 PCIe Gen4 x4 的链路上限,在 QD4 就已经达到。所以 QD1 只能拿 ~4.4 GB/s,QD4 及以上把链路打满,NAND 并行度和链路带宽在此同时见顶。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)

| 线程(在飞深度) | 聚合带宽 | 用时 |
|---|---|---|
| 1 | 4.46 GB/s | 962ms |
| 4 | 7.23 GB/s | 594ms |
| 16 | 7.11 GB/s | 604ms |

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
fio 对比:`--bs=4M --iodepth=1` vs `--bs=4M --iodepth=32 --ioengine=libaio`,看聚合带宽差异。

## 对 DingoFS 的意义
读缓存命中路径必须让多个块并发读(io_uring / 多个 IO in-flight),而不是一个 pread 等完再下一个;cache-perf/02-per-core-io-uring.md 的设计正是冲这个。

## 注意
压测单块延迟(QD1)和压测吞吐天花板(高 QD)是两回事,别混着看。这块盘 QD4 就到链路顶,继续加深度不再涨带宽——天花板是 PCIe 链路本身,不是队列深度;插在 Gen3 槽里则怎么压也上不去 3.5 GB/s。
