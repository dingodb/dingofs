# 07-numa-locality — NUMA 局部性(本单盘测不出)

> 一句话:从错误的 NUMA node 驱动 NVMe 可能加跨 socket 开销,但在这块单盘上没体现出来。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: local vs remote -> 1.00x [effect weak/none]。盘在 node1,本地(node1)和远端(node0)8 线程读都是 6.96 GB/s,单盘链路封顶,跨 socket 不是瓶颈。

## 这个 demo 测什么
盘在 NUMA node1。8 线程读并且真的去 touch(访问)读出来的数据:LOCAL(cpunodebind=1 membind=1) vs REMOTE(cpunodebind=0 membind=0)。

## 机制
单块 Gen4 x4 盘被链路封在 ~6.9 GB/s,远低于 socket 间 UPI 带宽(>20 GB/s),所以跨 socket 的 DMA 路径不是瓶颈;而且 demo 每字节的 CPU 处理很轻。NUMA 摆放真正咬人是在:(a) 多块盘聚合 IO 逼近 UPI 带宽,或 (b) 应用对 DMA 来、住在远端内存里的数据做重计算/memcpy/RDMA。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)

| 摆放 | 带宽 |
|---|---|
| LOCAL(cpunodebind=1 membind=1) | 6.96 GB/s |
| REMOTE(cpunodebind=0 membind=0) | 6.96 GB/s |
| local / remote | 1.00x |

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
`cat /sys/block/nvmeXn1/device/numa_node` 确认盘所在 node;再用 `numactl --cpunodebind` 在本地/远端各跑一遍带宽对比。

## 对 DingoFS 的意义
部署把进程 `--cpunodebind=0`,而缓存盘在 node1。对纯单盘 IO 带宽这没问题(本 demo 证明);只有在把 node1 上多块 NVMe 聚合到逼近 UPI 上限、或重的远端内存数据处理(校验/RDMA)成为主导时,才需要重新评估。(这修正了早先一个被夸大的担忧。)

## 注意
"weak/none" 是单块链路封顶盘的诚实结果;效果要在多盘聚合带宽、或受远端内存约束的计算下才会出现。绑 node 前务必确认绑的就是盘所在 node,绑错反而更慢。
