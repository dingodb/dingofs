# 04-burst-vs-sustained — 突发写 vs 持续写(本盘无悬崖)

> 一句话:短写突发被 SSD 的易失缓冲/SLC cache 吸收跑得飞快,持续写会掉到稳态速率;但这块企业盘没有悬崖。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: 第一个窗口 vs 最后一个窗口 -> 1.02x [effect weak/none]。整条曲线从第一个 GiB 起就是平的(每个 1GiB 窗口 4.42–4.55 GB/s),这块盘没有 burst→sustained 断崖。

## 这个 demo 测什么
连续写 16GiB,O_DIRECT bs=4MiB,按每写 1GiB 一个窗口统计带宽,看第一个窗口和最后一个窗口有没有掉速。

## 机制
P5510 是稳态 TLC + PLP DRAM 缓冲,没有消费盘那种动态 SLC 写缓存,所以写带宽从第一个 GiB 就是平的。消费/客户端盘有 SLC 写缓存:前几十 GB 写进 SLC 飞快,SLC 填满后断崖式掉到原生 TLC/QLC 速度——在那种盘上这个 demo 会画出明显的悬崖。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)
```
write 16GiB · O_DIRECT · bs=4MiB · 每 1GiB 一个窗口
每个窗口带宽:4.42–4.55 GB/s(基本一条直线)
first-window / last-window = 1.02x
```

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
持续写远超盘 DRAM 缓冲/SLC 容量的数据量(几十 GB),按时间窗口画带宽曲线;企业盘是直线,消费盘会看到中途断崖。

## 对 DingoFS 的意义
企业缓存盘上持续 Stage 吞吐 ≈ 这个平的数字,别指望突发加成,也别担心悬崖。换消费硬件时,容量/吞吐规划要用悬崖之后的稳态速率,不能用突发速率。

## 注意
"weak/none" 在这种盘上是正确且诚实的结果。要看到强版本,把这个 demo 跑在消费级 SSD 上(SLC 写缓存填满后会断崖);企业盘落差小,消费盘能掉 3~5 倍。
