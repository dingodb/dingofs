# 01-read-after-write — 读刚写完的同一块更慢

> 一句话:刚用 O_DIRECT 写下去的块、立刻读回同一块,比读一个已经回刷到 NAND 的块慢得多。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: 读刚写完的块 vs 读已回刷的块 -> 2.78x [EFFECT CONFIRMED]。读"最近 0~1 次写过的同一块"走控制器一致性慢路(~3ms),lag≥2 后块已回刷,读回到干净的 ~1.1ms。

## 这个 demo 测什么
循环里"写 f[i] → 读 f[i-lag]",O_DIRECT 4MiB,n=200。lag 就是读和写之间隔了几次写:lag=0 读刚写的同块,lag=1 读上一次写的,lag=4 读更早写的。设备全程一样忙,唯一变量是"读的块是多久以前写的"。

## 机制
O_DIRECT 写在数据进入控制器 PLP 保护的 DRAM 写缓冲时就 ack 了,但此时还没回刷到 NAND。立刻读这同一段 LBA 会撞上 read-after-write 一致性慢路(~3ms)。等 lag≥2(约 1~2 个写周期后)块已刷到 NAND,读就干净(~1.1ms)。各 lag 下设备写负载相同,所以这是"同块冒险",不是泛泛的"设备忙"。这是引出整个 tools/nvme 系列的最初发现(fsop-bench 因为每写一块就立刻读回它,量出 3ms 的读)。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)

| lag(读几次之前写的块) | read mean | read p99 |
|---|---|---|
| 0(刚写的同块) | 3176us | 3511us |
| 1(上一次写的) | 2421us | — |
| 4(4 次前写的) | 1141us | 1367us |

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
直接跑这个 demo;或在任何 benchmark 里把"写阶段"和"读阶段"彻底分开(全写完再读),read-after-write 慢路就消失了。

## 对 DingoFS 的意义
生产里 Stage 落盘的块由另一个 cache-hit 请求晚一些读回(lag≫2),所以没事;真正危险的是任何"Stage 完成后立刻同步读回同一块"的路径。

## 注意
只有读"最近 1~2 次写过的同一块"才慢,读更早写的块不受影响。基线别用规格峰值(4MiB ≈ 574us 是高 QD 峰值带宽),QD1 干净读本来就是 ~1ms 量级,别把它误当成"读比写慢"的设备特性。
