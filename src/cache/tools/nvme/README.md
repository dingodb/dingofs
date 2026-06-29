# tools/nvme — SSD 特性"眼见为实"对照实验集

每个子目录对应 SSD 的一个性能特性,用一个**能直接跑的 A/B 对照程序**(或观测脚本)让你**客观看到**这个特性确实影响性能。是 `src/cache/docs/ssd-io-characteristics.md` 那篇文档的"可执行版"——文档讲道理,这里给证据。

> 起因:`fsop-bench` 压测出"读 4MiB 要 3ms、写只要 700us"的反常。排查发现是 **read-after-write 同块惩罚 + QD1 不满盘**,不是盘坏。于是把每个相关 SSD 特性都做成一个可复现的小实验,沉淀在这。

## 怎么跑

```bash
# 单个特性(自动 sudo、自动编译、跑完清理):
cd 01-read-after-write && sudo ./run.sh

# 全部跑一遍(串行,几分钟;04/09 写得多):
sudo ./run_all.sh

# 换盘 / 换目录:
NVME_DIR=/mnt/cacheX/mytest sudo -E ./run_all.sh
```

- **纯 gcc + libc + nvme-cli/numactl/lspci**,不进 CMake,改完即跑。共享测时框架在 `common/nvme_bench.h`,shell 助手在 `common/lib.sh`。
- 需要 **root**:要 `drop_caches`、要写 root 属主的缓存目录、要 `nvme-cli`。脚本会自动 `sudo` 重入。
- 默认测试目录 `/mnt/cache0/wine93/nvme-demo`(一块真实 NVMe);跑完自动清理。
- `run.sh` = 跑对照程序;`observe.sh` = 只读观测(不改盘状态)。

## 19 个特性 + 本机实测结论(jg30 · Intel P5510 · Gen4 x4 · XFS/LVM · PLP)

| # | 特性 | 类型 | 本机实测结论 |
|---|------|------|------|
| 01 | [read-after-write 同块惩罚](01-read-after-write/) | 程序 | **2.78x** ✅ 刚写完读同块 3176us vs 已刷 1141us |
| 02 | [队列深度(QD1 不满盘)](02-queue-depth/) | 程序 | **1.59x** ✅ QD1 4.46 → QD4+ 6.9 GB/s |
| 03 | [混合读写互相干扰](03-mixed-read-write/) | 程序 | **2.24x** ✅ 读 927us → 写负载下 2078us |
| 04 | [突发 vs 持续写](04-burst-vs-sustained/) | 程序 | 1.02x 企业盘**无悬崖**(消费盘才明显) |
| 05 | [写对齐 / 部分块 RMW](05-write-alignment/) | 程序 | **2.04x** ✅ 4K 18.7us vs 512B RMW 38.2us;非对齐 O_DIRECT→EINVAL |
| 06 | [fsync 代价 ↔ PLP](06-fsync-plp/) | 程序 | +22us/op,vwc=0(PLP)→ 是 flush+元数据成本,非 NAND stall |
| 07 | [NUMA 局部性](07-numa-locality/) | 程序 | 1.00x 单盘链路封顶,**NUMA 在此不体现**(多盘聚合/远端内存计算才咬) |
| 08 | [尾延迟 / QoS](08-tail-latency-qos/) | 程序 | 1.35x 企业盘空闲尾延迟紧(有负载/消费盘才宽) |
| 09 | [写放大(小随机 vs 大顺序)](09-write-amplification/) | 程序 | **9.6x** ✅ 带宽差;lifetime WAF 1.46 |
| 10 | [盘越满越慢 / OP](10-capacity-op/) | 文档+观测 | 需长时间填满才显;给正确压测法 |
| 11 | [PCIe 链路天花板](11-pcie-link-ceiling/) | 观测 | Gen4 x4 → ~7GB/s,正好是 02 的上限 |
| 12 | [内核块层参数](12-kernel-block-layer/) | 观测 | sched=none,max_sectors_kb=128(4MiB拆32条) |
| 13 | [TRIM / discard](13-trim-discard/) | 观测 | ⚠️ **两块缓存盘都没开 TRIM**(该修) |
| 14 | [O_DIRECT ≠ 持久](14-odirect-durability/) | 文档+观测 | 概念 + blktrace 看 FLUSH 落盘 |
| 15 | [冷数据/老化读变慢](15-cold-data-retention/) | 文档+观测 | 需时间老化;给 SMART 观测 |
| 16 | [读扰动](16-read-disturb/) | 文档+观测 | 需海量重复读;给"读引发写"对照 |
| 17 | [read≪program≪erase](17-nand-latency-hierarchy/) | 文档+观测 | 解释 01/03/04/08 的物理根源 + 磨损 |
| 18 | [多流/FDP/ZNS](18-data-placement-fdp-zns/) | 文档+观测 | 本机普通盘**不支持**;讲收益+检测 |
| 19 | [易失写缓存开关](19-write-cache-toggle/) | 文档+观测 | PLP 盘无 VWC 可调 |

✅ = 在这块企业盘上就跑出了明显差异。04/07/08 的"弱"是**诚实结果**:好的数据中心盘本来就抗这几样,README 里说明了消费盘/有负载时才强。

## 在这块盘上"咬得最狠"的几个

1. **写放大**(09,9.6x)、**read-after-write**(01,2.78x)、**混合读写**(03,2.24x)、**部分块 RMW**(05,2x)、**QD1 不满盘**(02,1.6x)。
2. 两条**该修的运维项**:`13` 缓存盘没开 TRIM;`11/12` 确认链路是 Gen4 x4、调度器 none。
3. 一条**被纠正的误解**:`07` DingoFS 绑 node0、盘在 node1,对**单盘 IO 带宽无影响**(之前担心夸大了)。

## 数据出处

所有"实测"数字是 2026-06-29 在 jg30(=aurora-dingofs-0002)的 `/mnt/cache0`(Intel D7-P5510)上跑出来的。**换硬件需重测**;规律是通用的,绝对值随盘/文件系统/内核而变。
