# 08-tail-latency-qos — 尾延迟与 QoS(空闲下很弱)

> 一句话:mean 会掩盖偶发的延迟尖刺,真正伤 SLA 的是尾部(p99.9/max)。

> 硬件:jg30(aurora-dingofs-0002) · Intel/Solidigm D7-P5510 7.68TB(SSDPF2KX076T9K, PCIe Gen4 x4, 带 PLP) · XFS on LVM-linear(单盘 nvme3n1) · scheduler=none · max_sectors_kb=128 · 盘在 NUMA node1 · 实测 2026-06-29

## 结论(jg30 实测)
VERDICT: max vs p50 -> 1.35x [effect weak/none]。这块企业盘空闲纯读的尾延迟很紧(max 只有 median 的 ~1.35x);尾部要在持续混合读写下才会张开。

## 这个 demo 测什么
5000 次 QD1 O_DIRECT 128KiB 冷读,统计 mean/p50/p99/p99.9/max,看尾部相对中位数高出多少。

## 机制
这块企业盘 QoS 很好——空闲时纯读尾延迟很紧(max 仅 median 的 ~1.35x)。尾延迟尖刺会在持续混合读写负载下变大(后台 GC 来抢——见 demo 03,写负载把读延迟翻倍),消费盘 QoS 更弱时也更毛刺。永远报 p99/p99.9,别只报 mean。

## 实测数据(2026-06-29 · jg30 · Intel P5510 · XFS/LVM)
```
5000 × QD1 · O_DIRECT · 128KiB · 冷读
mean=326.8us  p50=375.0us  p99=436.7us  p99.9=455.3us  max=505.3us
max / p50 = 1.35x
```

## 怎么跑
```bash
sudo ./run.sh
```
（依赖:gcc + 一块真实 NVMe 盘;默认测试目录 /mnt/cache0/wine93/nvme-demo,可用 NVME_DIR=/path 覆盖。需要 root:drop_caches + 写 root 属主的缓存目录。）

## 怎么在自己机器上观测
跑这个 demo 看 p99.9/max;或一边跑 demo 03 的写负载一边重测,看尾部张开;消费 SSD 上也更明显。

## 对 DingoFS 的意义
缓存读的容量/SLA 规划应盯着 p99.9,并预期尾部会在并发写/上传负载下变宽,而不是在空闲时。

## 注意
空闲下尾部很紧是这块企业盘的诚实结果;要看到宽尾,在 demo 03 的写负载下重跑,或换 QoS 更弱的消费 SSD。
