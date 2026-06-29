# 03-mixed-read-write — 混合读写:写流抬高读延迟

> 一句话:盘同时在吸收写入时,一个读的延迟会被明显拖高。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: 有写负载下的读延迟 vs 空闲读 -> 2.24x [EFFECT CONFIRMED]。2 个写线程猛灌时,QD1 读延迟从 ~0.93ms 翻到 ~2.08ms。

## 这个 demo 测什么
QD1 O_DIRECT 4MiB 读延迟,两种背景:A) 没有并发写;B) 2 个写线程持续猛写。

## 机制
前台读要和控制器内部在飞的回刷/NAND program(某些盘还有 erase)抢资源,读得排在设备内部的写工作后面才能完成。(相关:盘支不支持 program/erase suspend 决定读尾延迟会被拖多惨——见源文档 §6.2。)

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)

| 场景 | read mean | read p99 |
|---|---|---|
| A 无并发写 | 927us | 1051us |
| B 2 个写线程猛写 | 2078us | 2730us |

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
一个进程做 QD1 4MiB 顺序读测延迟,同时另起 1~2 个进程对同盘持续顺序写,对比读延迟有没有翻倍。

## 对 DingoFS 的意义
后台上传/回刷的写流可能抬高前台读命中的尾延迟;可考虑给后台写限速,或读写分流来保住读 QoS。

## 注意
这里只跑 QD1 单读;混合负载越重、写突发越猛,读尾延迟尖刺越明显(尤其控制器不支持 erase suspend 的盘)。报延迟要看 p99,不要只看 mean。
