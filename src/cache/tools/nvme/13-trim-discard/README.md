# 13-trim-discard — 删了要告诉盘(TRIM)

> 一句话:文件系统释放的块若不下发 TRIM,SSD 一直当它有效数据去 GC,越用越慢。

观测项:检查缓存盘是否在做 TRIM(`discard` 挂载选项 或 周期 `fstrim.timer`)。

## 结论(jg30 实测 —— 当前是"裸奔")
```
fstrim.timer: enabled=disabled  active=inactive     # 周期 TRIM 没开
/mnt/cache0   xfs   rw,relatime,...,noquota          # 挂载无 discard
/mnt/cache1   xfs   rw,relatime,...,noquota          # 挂载无 discard
nvme3n1 / cache0   DISC-MAX=2T                        # 盘本身支持 TRIM
```
**两块缓存盘 discard 和 fstrim 都没有 —— TRIM 实际从未发生。** 盘和 LVM 栈都支持,纯属没配。

## 机制
缓存 LRU 淘汰 = `unlink`,块在文件系统层释放了,但没 TRIM,SSD 的 FTL 并不知道这些块空了,会一直当有效数据搬来搬去(GC)。盘跑得越久、churn 越多 → 写放大(WAF,见 `09-write-amplification`)越涨 → 稳态写吞吐掉、尾延迟涨。

## 怎么跑
```bash
sudo ./observe.sh        # 只观测,不改盘状态
```

## 怎么修(用周期 fstrim,别用 inline discard)
```bash
# 方案 A:systemd timer(默认每周,可改 daily)
sudo systemctl enable --now fstrim.timer
# 方案 B:cron,每天空闲时段 trim 两块缓存盘
30 3 * * *  /usr/sbin/fstrim -v /mnt/cache0 /mnt/cache1
```
- **不建议 inline `discard` 挂载选项**:它每次 `unlink` 同步下发 TRIM,给淘汰热路径加延迟、重 churn 时给前台 IO 制造尖刺。batched fstrim 不碰热路径。
- ⚠️ **首次 trim 会很大**(积压已久),挑完全空闲时跑。

## 对 DingoFS 的意义
这是 `src/cache/docs/ssd-io-characteristics.md` §5 检查清单里最值得修的一条。当前缓存盘 churn 在跑、却没 TRIM → 写性能随时间劣化。

## 注意
`observe.sh` 故意**不**自动跑 `fstrim`(它会改盘状态 + 首次有 IO 尖刺)。确认要修时自己执行上面的命令。
