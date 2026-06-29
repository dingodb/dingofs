# 14-odirect-durability — `O_DIRECT` ≠ 持久

> 一句话:O_DIRECT 只是绕过 page cache,**不保证落盘**;要持久必须显式 fsync。

这是个**概念 + 正确性**项,没有 A/B 程序——因为要"证明丢数据"得真拔电,不能在共享环境里做。这里讲清楚为什么、以及怎么验证你的写真的持久了。

## 机制
- `O_DIRECT` 的语义是"绕过内核 page cache,DMA 直达设备",**不等于**数据已经安全落到非易失介质。在没有 PLP 的盘上,数据可能还停在盘的易失写缓冲里(正是 `01-read-after-write` 慢的原因之一:数据还没进 NAND)。崩溃/掉电照样丢。
- 要持久,必须显式 `fsync`/`fdatasync`(或 `O_SYNC`/写时带 FUA)。
- **别忘了目录项**:用 tmp+rename 落文件后,**还要 fsync 父目录**,否则崩溃后文件可能"消失"(目录项没落盘)。

## 怎么验证(观测,不毁数据)
```bash
# 用 blktrace 看一次 O_DIRECT 写后有没有 FLUSH/FUA 到设备:
sudo blktrace -d /dev/nvme3n1 -o - | blkparse -i - | grep -iE 'FLUSH|FUA'
#   只有 pwrite、没有 FLUSH  -> 数据未必持久
#   fdatasync 后出现 FLUSH/FUA -> 真正落盘
```
(没有 blktrace 时,概念上记住:O_DIRECT 写返回 ≠ 已持久。)

## 对 DingoFS 的意义
Stage 用 tmp+rename 模式落块。这条路径的崩溃一致性要看清:① 数据 fsync;② rename 后父目录 fsync;③ 目标盘有没有 PLP(见 `06-fsync-plp`)。PLP 盘上 fsync 便宜,但**逻辑上仍必须调**。

## 注意
PLP 盘(本机 P5510)上"O_DIRECT 写返回即在掉电安全域",所以实测 read-after-write 不会丢——但这是**盘的 PLP 在兜底**,不是 O_DIRECT 的保证。换没 PLP 的盘,同样代码可能丢数据。
