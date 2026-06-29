# 17-nand-latency-hierarchy — read ≪ program ≪ erase

> 一句话:NAND 单元级延迟,读最快、写(program)慢一个量级、擦(erase)更慢——写天生比读贵。

**概念**项。这些是 NAND 内部延迟,控制器把它们用缓冲/并行/流水线掩盖了,没法直接在文件系统层单独计时;但它们解释了本目录好几个 demo 的现象。

## 机制(单元级,约数)
| 操作 | 量级 | 说明 |
|---|---|---|
| read    | ~50–100 us | 最快 |
| program(写) | ~几百 us – 几 ms | TLC 要多遍编程,**上层页比下层页慢得多** |
| erase(擦) | ~几 ms / 块 | 最慢,且按"块"(MB 级)为单位 |

TLC(3 bit/cell)多遍编程、QLC(4 bit)更慢更不耐写。**写在物理上就比读贵一个量级**,平时被 DRAM 缓冲掩盖(所以你看到"写很快"),一旦缓冲耗尽就露出真实高写延迟。

## 这个量级等级解释了哪些 demo
- `04-burst-vs-sustained`:缓冲掩盖了 program 的慢;缓冲耗尽 → 掉到 sustained(消费盘明显)。
- `03-mixed-read-write`:读排在 program/erase 后面被拖慢。
- `01-read-after-write`:读一个刚写、还没 program 进 NAND 的块,走慢路。
- **program/erase suspend**:好控制器能暂停正在进行的 erase(几 ms)先放读过去,否则读尾延迟会被一次 erase 顶到几 ms(`08-tail-latency-qos` 在有负载时会放大)。

## 对 DingoFS 的意义
选缓存盘看 **TLC 还是 QLC**、看**稳态(不是峰值)写规格**;QLC 盘别拿来做高写入的缓存。理解"写贵、擦更贵"有助于解释为什么写路径优化(批量、顺序、少擦)比读路径更值钱。

## 注意
无法在文件系统层单独测出这三个值(被控制器掩盖);本项是把其它 demo 的现象归因到同一物理根源。
