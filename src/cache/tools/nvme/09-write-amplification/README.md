# 09-write-amplification — 写放大:小随机写是最坏情况

> 一句话:小随机写是 SSD 的最坏工况——每 host 字节远慢于大顺序写,还会放大 NAND 写入(磨损 + 降速)。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: 顺序 vs 随机 4k 带宽 -> 9.6x 更快 [EFFECT CONFIRMED]。1MiB 顺序写 4.49 GB/s,4KiB 随机写只有 0.47 GB/s;盘的 lifetime 写放大 WAF = 1.46。

## 这个 demo 测什么
每种模式写 8GiB host 字节,O_DIRECT:rand4k(4KiB 随机) vs seq1m(1MiB 顺序),对比带宽;再读盘的 lifetime 写放大。

## 机制
分散的小写逼控制器在 GC 时搬移大量有效数据才能腾出可擦除块(写放大 = NAND 写字节 / host 写字节),既磨损盘又压低持续写吞吐。大顺序写填满整个擦除块 → WAF ≈ 1。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)
```
每模式写 8GiB host 字节 · O_DIRECT
rand4k (4KiB 随机):   0.47 GB/s (18.3s)
seq1m  (1MiB 顺序):   4.49 GB/s (1.9s)
seq / rand4k = 9.6x 更快
drive lifetime WAF(nvme intel smart-log-add: nand/host) = 1.46
```

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
`nvme intel smart-log-add /dev/nvmeX` 看 nand_bytes_written / host_bytes_written;或直接对比 4k 随机写 vs 顺序写带宽。

## 对 DingoFS 的意义
整块写 4MiB(近顺序)是对的;避免零碎散落的元数据/索引小写。

## 注意
单工况 WAF 在共享/繁忙盘上测不准——厂商 nand 计数滞后约 10s 且含后台 GC。要干净测 WAF:空闲盘、写一个固定大体量、静置 ~60s,再取 `nvme intel smart-log-add` 的 Δnand/Δhost。这里报的是 lifetime 比值。
