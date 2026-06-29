# 05-write-alignment — 写对齐:partial 写触发读-改-写

> 一句话:写一个不完整的设备块会触发读-改-写(RMW),而 O_DIRECT 直接拒绝不对齐的 IO。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: partial 块(RMW) vs 整块写 -> 2.04x [EFFECT CONFIRMED]。512B partial 写(38.2us)是 4KiB 整页写(18.7us)的两倍多;O_DIRECT 不对齐直接 EINVAL。

## 这个 demo 测什么
A) 4KiB 整页对齐写(无 RMW);B) 512B partial 写进冷页(触发 RMW);外加一条硬规则:O_DIRECT 做不对齐 pread 直接报错。

## 机制
sub-block 的写不能直接落:要先把周边整块读出来、把你的字节拼进去、再整块写回(读-改-写)。整块对齐写跳过那次读。此外 O_DIRECT 本身就要求 4K 对齐(缓冲区、偏移、长度),否则返回 EINVAL。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)
```
O_DIRECT 不对齐 pread -> rc=-1 errno=22 (Invalid argument)   # 硬规则
A) 4KiB 整页对齐写(无 RMW):    mean=18.7us  p99=99us
B) 512B partial 写进冷页(RMW):  mean=38.2us  p99=164us
partial / full = 2.04x
```

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
对比 4K 对齐写 vs sub-4K(如 512B)写的延迟;或对一个不对齐的缓冲区/偏移做 O_DIRECT,看 EINVAL(errno=22)。

## 对 DingoFS 的意义
块写保持 4K 对齐、整块写;避免零碎散落的 sub-block 元数据/索引小写。

## 注意
内部页 4KB/16KB、擦除块 MB 级,对齐越大越省。这里的 2x 是 4KiB vs 512B 的对比;partial 越碎、跨越的块越多,RMW 惩罚越重。
