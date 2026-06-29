# 12-kernel-block-layer — 内核块层参数怎么影响实际性能

> 一句话:盘很快,但内核块层的拆分/调度/中断方式决定你实际拿到多少。

观测项:dump 盘的块层关键参数(`/sys/block/<dev>/queue/*`)。

## 结论(jg30 实测)
```
scheduler        = [none] mq-deadline kyber bfq   # 选中 none,对 NVMe 最优
max_sectors_kb   = 128       # 一个 4MiB 请求被拆成 4096/128 = 32 个设备命令
max_hw_sectors_kb= 128       # 硬件上限就是 128KB/命令
nr_requests      = 1023      # 每队列在途请求上限
read_ahead_kb    = 128       # 仅影响 buffered IO;O_DIRECT 绕过
io_poll          = 0         # 中断驱动;未开轮询
hw queues        = 128       # blk-mq 多队列(每核一个)
```

## 机制(每个旋钮怎么咬人)
- **`max_sectors_kb`**:单个大 IO 被切成多少个设备命令。128 → 4MiB 拆 32 条。这也是为什么 `02-queue-depth` 里一次 4MiB 的 `pread` 在设备侧就已经有 32 条在途。
- **调度器**:NVMe 用 `none`(noop)最好;`mq-deadline`/`kyber`/`bfq` 给本就低延迟的 NVMe 反而加开销。
- **`read_ahead_kb`**:只对 buffered 读有用;O_DIRECT 完全绕过,所以缓存盘 O_DIRECT 路径调它没用。
- **中断 vs 轮询(`io_poll`)**:超高 IOPS 下中断开销可观。io_uring 的 **SQPOLL/IOPOLL** 用轮询省掉 syscall+中断,小 IO 延迟更稳。

## 怎么跑
```bash
sudo ./observe.sh
```

## 怎么调(谨慎,临时生效)
```bash
echo none  | sudo tee /sys/block/nvme3n1/queue/scheduler        # 选调度器
echo 1024  | sudo tee /sys/block/nvme3n1/queue/max_sectors_kb   # 增大单命令(<=max_hw)
```

## 对 DingoFS 的意义
io_uring 路径(`cache-perf/02-per-core-io-uring.md`)能不能开 SQPOLL/注册 buffer,直接决定小 IO 天花板。确认调度器是 `none`、IO 大小与 `max_sectors_kb` 的拆分关系,有助于解读分层压测数字。

## 注意
这些是主机侧设置,不是盘的物理特性;改它们是"软"调优。`max_hw_sectors_kb` 是硬件上限,`max_sectors_kb` 不能超过它。
