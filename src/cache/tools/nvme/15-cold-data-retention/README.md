# 15-cold-data-retention — 冷数据 / 老数据读会变慢

> 一句话:NAND 电荷随时间漂移,放久的数据误码率升高,读要更多纠错、甚至 read-retry,变慢。

**概念 + 观测**项,没法在一次会话里跑出来(要数据真正"放几周/几个月"老化)。这里讲机制 + 怎么通过 SMART 观测盘的磨损/纠错趋势。

## 机制
- NAND 单元靠囚禁电荷存 bit,电荷会随**时间和温度**缓慢泄漏。数据越老,读出来的原始误码率越高。
- 控制器先用 LDPC/ECC 软解码纠;纠不过来就触发 **read-retry**(换不同读电压重读一次甚至多次)——一次 read-retry 能让该次读延迟翻几倍。
- 结果:**同一块盘,刚写的数据读得快,放了几个月的冷数据读得慢、尾延迟更毛刺。**

## 怎么观测
```bash
sudo ./observe.sh        # 打印 media_wear / retry 相关 SMART 计数
# 关注:retry_buff_overflow_count、media_wear_percentage、percentage_used 随时间的变化
```
没有直接的"冷数据读延迟"计数器;只能看磨损/重试趋势 + 长期对比"新写 vs 老数据"的读延迟分布。

本机实测基线(2026-06-29,盘很新):
```
percentage_used = 0%   media_errors = 0   num_err_log_entries = 0
retry_buff_overflow_count = 0            media_wear_percentage(raw normalized) = 100
temperature = 109°F
```
全 0 / 全新 —— 没有老化压力。这就是"基线",几周/几月后再跑对比 retry/wear 是否上涨。

## 对 DingoFS 的意义
基本是控制器的事、程序层无解。但它解释了一个现象:**压测刚写的数据 vs 线上跑了很久的老缓存块,读延迟分布不一样**——别拿全新数据的读延迟当线上老数据的承诺。缓存数据本就在不断重写,受影响相对小。

## 注意
本项**无法**用 demo 复现(需要真实时间老化)。`observe.sh` 只给磨损/重试的快照,作为长期跟踪的起点。
