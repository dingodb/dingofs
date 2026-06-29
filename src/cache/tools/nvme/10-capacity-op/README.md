# 10-capacity-op — 盘越满越慢 + 过度配置(OP)

> 一句话:盘越满,GC 越没空闲块腾挪,写放大越高、写越慢;留空间 = 加 OP = 提速。

这是个**文档 + 观测**项。要在程序里清楚跑出来需要把**整盘填满 + 长时间预处理**(几十分钟~小时级),不适合"一跑就出",所以这里给观测脚本 + 正确的压测方法,而不是秒级 A/B。

## 机制
GC 要腾出空闲擦除块才能写新数据。盘越满、空闲块越少,GC 每腾一个块就得搬越多有效数据 → **写放大(WAF)飙升、写吞吐掉、尾延迟涨**。一块 90% 满的盘,稳态随机写可能只有 60% 满时的一半。**过度配置(OP)**= 给盘留一截永不写的空间(出厂 OP,或自己只分区 80%),等于给 GC 更多腾挪余地,稳态写性能和延迟一致性显著变好。

## 怎么观测当前水位
```bash
sudo ./observe.sh        # 打印缓存盘的 used%,越接近满风险越大
```

## 怎么正确压出这个效应(需要时间)
```bash
# 1) 低填充基线:在几乎空的盘上随机写,测稳态写延迟
fio --name=low --filename=/mnt/cacheX/f --rw=randwrite --bs=4k \
    --size=20G --direct=1 --runtime=120 --time_based
# 2) 把盘填到 ~90%(写一个大文件占住空间),再重复同样的随机写
fio --name=fill --filename=/mnt/cacheX/fill --rw=write --bs=1M --size=<~85%容量> --direct=1
fio --name=high --filename=/mnt/cacheX/f --rw=randwrite --bs=4k \
    --size=20G --direct=1 --runtime=120 --time_based
# 对比 high vs low 的 IOPS / p99:高填充明显更差
```

## 对 DingoFS 的意义
缓存盘按 LRU 填到接近满 + 持续 churn,正是 WAF 最高的工况。`--free_space_ratio` 这类水位线**不只是防写满,也直接影响写性能**——别设太激进;必要时只分区 ~80% 加大 OP。

## 注意
本机缓存盘当前很空(~1% used),所以现在测不出这个效应;它在**长期跑到接近满**后才显现。企业盘出厂 OP 高、落差小;消费盘落差大。
