# 06-fsync-plp — fdatasync 的代价取决于 PLP

> 一句话:fdatasync() 有多贵,取决于盘有没有掉电保护(PLP)。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
drive 报 vwc=0(无易失写缓存 => 有 PLP)。+fdatasync 每次给写加 ~22us(1.88x)——这是固定开销,不随 NAND 编程放大;换没 PLP 的盘会贵得多。

## 这个 demo 测什么
盘先报告 vwc(易失写缓存)状态。然后做 O_DIRECT 64KiB 写两种:A) 不 sync;B) 每次写后 fdatasync,对比延迟。

## 机制
在带 PLP 的数据中心盘上,数据进了控制器受保护的 DRAM 就算持久,所以 fdatasync 不会去等 NAND。这里的 ~22us 是固定成本:FLUSH 命令的往返 + XFS 元数据(文件大小)日志提交——不是 NAND 编程的停顿。换成 vwc=1、没 PLP 的消费盘,fdatasync 会把易失缓存真正逼出到 NAND,代价高得多,持续写负载下尤甚。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)
```
drive 报告:vwc=0(无易失写缓存 => PLP)
O_DIRECT 64KiB 写:
A) 不 sync:         write mean=25.3us
B) +fdatasync 每次:  write mean=47.5us   (fdatasync 加 ~22us/op, 1.88x)
```

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
`nvme id-ctrl /dev/nvmeX | grep vwc`(vwc=0 暗示 PLP),再对比 write vs write+fdatasync 的延迟。

## 对 DingoFS 的意义
Stage 的持久化成本在 PLP 缓存盘上很便宜,但在没 PLP 的硬件上可能完全不同——要在实际目标盘型上压测 fsync 成本,别假设它便宜也别假设它贵。

## 注意
这里的 1.88x 不等于"fsync 很贵"——它是固定的 ~22us、大头来自文件系统元数据提交;重点是它不像没 PLP 的盘那样随 NAND 编程放大。
