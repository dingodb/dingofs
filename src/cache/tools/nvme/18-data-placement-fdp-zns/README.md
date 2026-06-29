# 18-data-placement-fdp-zns — 多流 / FDP / ZNS:从根上压低写放大

> 一句话:把"生命周期相近的数据放一起"告诉盘,GC 时整块回收、几乎零搬运,WAF 趋近 1。

**概念 + 观测**项,前瞻特性。需要盘和内核都支持才能用;本机这块 P5510 不支持(见下),所以是"了解 + 检测支持",不是 demo。

## 机制
写放大(WAF,见 `09`)高的根因:**不同生命周期的数据被混在同一个擦除块**——长寿数据和马上要删的数据混着,GC 时被迫一起搬。现代手段让主机显式分组:
- **NVMe Streams**:给写打 stream id,同 id 的数据放一起。
- **FDP(Flexible Data Placement)**:更通用的放置提示,host 控制数据落到哪个 reclaim unit。
- **ZNS(Zoned Namespaces)**:盘暴露"顺序写的 zone",host 按 zone 管理 → 几乎零盘内 GC。
三者都能把 GC 搬运降到接近 0 → WAF≈1、稳态写更快更稳、盘更耐写。

## 怎么观测支持情况
```bash
sudo ./observe.sh
```
本机实测:`ctratt=0`(无 FDP)、Streams directive 未启用、ZNS/`fdp configs` 返回 Invalid Field —— **P5510 是普通(conventional)盘,三者都不支持。**

## 对 DingoFS 的意义
缓存里"热块数据 vs 元数据/索引"生命周期差异大,理论上很适合按生命周期分流(Streams/FDP)。属于"想把缓存盘寿命和稳态写做到极致"时、且**换支持 FDP/ZNS 的盘**后才动的高级牌;当前硬件用不上。

## 注意
是否可用完全取决于硬件 + 内核版本。本目录只检测支持、解释收益,不在不支持的盘上强行启用。
