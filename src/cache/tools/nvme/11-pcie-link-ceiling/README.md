# 11-pcie-link-ceiling — PCIe 代数/通道是带宽硬上限

> 一句话:再快的 NAND 也跑不过 PCIe 链路;先确认盘到底协商到了 Gen 几 / x 几。

这是个**观测**项(没有 A/B 程序):用 `lspci` 看盘的链路能力(LnkCap)和实际协商值(LnkSta)。

## 结论(jg30 实测)
```
LnkCap: Speed 16GT/s, Width x4    (Gen4 x4 能力)
LnkSta: Speed 16GT/s (ok), Width x4 (ok)   (实际就跑在 Gen4 x4)
```
Gen4 x4 可用带宽上限 ≈ **7 GB/s**,这正好解释了 `02-queue-depth` 高并发只能到 **6.9 GB/s**——不是代码不行,是**链路封顶了**,NAND 并行度和链路带宽在这同时见顶。

## 机制
PCIe 链路带宽 = 代数 × 通道数。每代每通道翻倍:Gen3 ≈ 1 GB/s/lane,Gen4 ≈ 2,Gen5 ≈ 4。常见盘是 x4:
| 链路 | 可用带宽(约) |
|---|---|
| Gen3 x4 | ~3.5 GB/s |
| Gen4 x4 | ~7 GB/s |
| Gen5 x4 | ~14 GB/s |
如果盘插在更慢的槽、或被 BIOS/主板降速(downtrain),`LnkSta` 会低于 `LnkCap`,你就被卡在盘能力以下。

## 怎么跑
```bash
sudo ./observe.sh
```

## 怎么在自己机器上观测
```bash
# 找到盘的 PCI 地址再看链路
lspci -vv -s <bus:dev.fn> | grep -E 'LnkCap:|LnkSta:'
```
`LnkSta` 的 Speed/Width 若带 `(downgraded)` 或低于 `LnkCap`,就是链路出了问题。

## 对 DingoFS 的意义
压测缓存盘读带宽不及预期时,**先查 `LnkSta` 是不是 Gen4 x4**,再怀疑缓存代码。本机 4 块盘都在 Gen4 x4,单盘天花板 ~7 GB/s。

## 注意
这是硬件事实、不随负载变化;一次确认即可。多盘聚合带宽还会受 CPU 的 PCIe root / UPI 限制(见 `07-numa-locality`)。
