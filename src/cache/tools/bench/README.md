# cb — DingoFS 缓存分层压测工具

`cb`（cache-bench）把缓存栈每一层的压测合并成**一个二进制**，每层一个子命令：

```
cb aio      io_uring/AioQueue 裸异步盘 I/O
cb fs       LocalFileSystem 整块 WriteFile/ReadFile
cb store    DiskCache / MemCache / LocalBlockCache 存储层
cb client   TierBlockCache 完整客户端栈（需要 mds）
cb fsop     单 syscall 耗时拆解（open/rename/fallocate/...）
```

构建后位于 `build/bin/cb`。

## 帮助

```bash
cb                 # 顶层命令列表
cb <cmd> --help    # 某个子命令的全部选项
cb help <cmd>      # 等价写法
```

## 分层压测方法论（自底向上做消融）

缓存栈是层层叠加的：`AioQueue → LocalFileSystem → CacheStore → TierBlockCache`。
要定位性能瓶颈，就**从最底层开始逐层加压**，把每一层单独测出来，再看相邻两层的差值是哪一层引入的开销：

| 层 | 子命令 | 测的是什么 | 排除了什么 |
|----|--------|-----------|-----------|
| 1 | `cb aio` | 裸盘 + io_uring 的极限带宽/IOPS | 文件系统封装、缓存逻辑 |
| 2 | `cb fs` | 整块文件读写（含建目录/重命名等） | 缓存索引、淘汰、S3 |
| 3 | `cb store` | DiskCache/MemCache/BlockCache 的存取 | mds、网络、远端缓存 |
| 4 | `cb client` | 端到端客户端（命中本地/远端/回源 S3） | 无（最完整） |

如果 `fs` 明显比 `aio` 慢，瓶颈在文件系统封装；如果 `store` 明显比 `fs` 慢，瓶颈在缓存索引/淘汰逻辑，以此类推。

所有 I/O 类子命令（aio/fs/store/client）都是**开环（open-loop）**压测：按目标 QPS 投递，延迟从“计划投递时刻”起算，避免协调遗漏（coordinated omission），p99/p999 才可信。

## 常用命令

```bash
# 1) 裸盘极限：4K 随机读，128 个并发 worker（= 128 个在飞 I/O），跑 30s
cb aio --dir=/data/t --rw=randread --bs=4KiB --iodepth=128 --runtime=30
# 1b) 并发(在飞 I/O 数)由 --threads 决定，每个 worker 闭环压 1 个 I/O 等完再发下一个；
#     --iodepth 只设 io_uring ring 深度（= 现在 cache 里 FLAGS_iodepth 的用法）。
#     下面 = 32 并发、ring 深 128。想量单次延迟就 --threads=1。
cb aio --dir=/data/t --rw=randread --bs=4KiB --iodepth=128 --threads=32 --runtime=30

# 2) 文件系统层：1M 整块随机读，4096 个块文件，64 并发
cb fs --dir=/data/t --rw=randread --bs=1MiB --nrfiles=4096 --iodepth=64

# 3) 存储层：DiskCache 随机读（mem / blockcache 同理）
cb store --layer=diskcache  --dir=/data/t --rw=randread --bs=1MiB --iodepth=64
cb store --layer=blockcache --dir=/data/t --rw=randread --nrfiles=4096
cb store --layer=mem        --rw=randread --bs=64KiB --nrfiles=8192

# 4) 端到端：zipf 热点 key，目标 2w QPS，跑 60s（前 10s 预热不计入）
cb client --mds_addrs=127.0.0.1:7400 --fsid=1 --op=range \
          --key_dist=zipf --keyspace=100000 --qps=20000 --duration=60 --warmup=10
```

## fsop — 元数据 syscall 拆解

`cb fsop` 不测带宽，而是把一次“写一个块文件再读回”拆成 `mkdir/open/fallocate/write/rename/link/unlink/...`
逐个 syscall 计时，输出每个操作的 mean/p50/p99 及占比，并做小 IO/大 IO × 单线程/多线程的矩阵对照：

```bash
cb fsop --dir=/data/t --sizes=4KiB,4MiB --threads=1,8 --iters=500
```

线程数增加时元数据耗时上升，主要源于底层文件系统（如 XFS）的**目录 inode 锁**（`i_rwsem`）和日志/AG 锁竞争。
用 `--shared_dir` 让所有线程共用一棵目录树，即可隔离出这部分目录锁开销（与每线程独立目录对照）：

```bash
cb fsop --dir=/data/t --sizes=4KiB --threads=1,8,32 --shared_dir
```

## 火焰图（`--flamegraph`）

定位瓶颈时，光看延迟分位数只能「猜」。给任意一次压测加上 `--flamegraph`，
`cb` 会在**测量窗口**内自动抓火焰图，跑完后渲染成可交互的 SVG、起一个 web 服务并打印链接：

- **on-cpu**（`perf`）：CPU 时间花在哪些函数/哪一层；
- **off-cpu**（bcc `offcputime`）：线程在哪里**阻塞/等待**（等锁、等 IO、被抢占），计数=**绝对微秒**，io 配色；
- **lock**（brpc 进程内 contention profiler）：每条调用链**在 mutex 上阻塞的绝对时间**（us），定位锁瓶颈最直接；**进程内采集，不需要 root/perf/bcc**。

> off-cpu 与 lock 火焰图的**计数就是绝对时间(us)**，宽度=占比；**鼠标悬停**任一帧即看该调用链消耗的绝对微秒，输出行也会打印总时长。off-cpu 覆盖一切阻塞（锁+IO+上下文切换），lock 只看 mutex 但更精准、零依赖。

渲染（`perf script` 折叠 + 符号化 + SVG 生成）与 web 服务都**内置在 cb 二进制里**（原生 C++，
无 perl / python3 / 外部脚本）。支持 `aio` / `fs` / `store` / `client`（`fsop` 是 syscall 矩阵、无稳态窗口，**不支持**）。

```bash
# 默认只抓 on-cpu（依赖最少：perf+addr2line）；结束后打印 http 链接，浏览器打开即看
cb aio --dir=/data/t --rw=randread --bs=4KiB --iodepth=128 --runtime=30 --flamegraph

# 只看 on-cpu，采样 199Hz，固定端口
cb store --layer=blockcache --dir=/data/t --rw=randread --runtime=30 \
         --flamegraph --profile_mode=on_cpu --profile_freq=199 --profile_port=8088

# 锁竞争火焰图（进程内、无需 root/perf/bcc，只需 addr2line 符号化）
cb store --layer=mem --rw=randread --nrfiles=8192 --jobs=32 --runtime=20 \
         --flamegraph --profile_mode=lock

# 三张一起：on-cpu + off-cpu + lock
cb store --layer=blockcache --dir=/data/t --rw=randread --runtime=30 --flamegraph --profile_mode=all
```

输出示例：

```
[flamegraph] on-cpu  : /tmp/cb-flame-12345/on_cpu.svg
[flamegraph] off-cpu : /tmp/cb-flame-12345/off_cpu.svg  (total blocked ≈ 820 ms across threads; hover a frame for its us)
[flamegraph] lock    : /tmp/cb-flame-12345/lock.svg  (total on locks ≈ 11498 ms; hover a frame for its us)
[flamegraph] serving : http://10.0.0.3:43187/   (Ctrl-C to stop)
```

| flag | 默认 | 说明 |
|------|------|------|
| `--flamegraph` | false | 总开关；`--noflamegraph` 关闭 |
| `--profile_mode` | `on_cpu` | `on_cpu` / `off_cpu` / `lock` / `both`(=on+off) / `all`；可逗号组合，如 `on_cpu,lock` |
| `--profile_freq` | 99 | on-cpu 采样频率 (Hz) |
| `--profile_dir` | 空 | 输出目录；空 = `/tmp/cb-flame-<pid>` |
| `--profile_port` | 0 | http 端口；0 = 自动选空闲端口 |

**依赖**（压测机）：
- **on-cpu**：`perf`（+ `binutils`/`addr2line` 做 DWARF 符号化，`perf` 包通常已带）；
- **off-cpu**：`bcc-tools`（`offcputime`，需 **root** + BTF）；
- **lock**：仅需 `addr2line`（binutils）符号化，**进程内采集、不需要 root/perf/bcc**——容器里也能跑。

一键装齐：`dnf install -y perf binutils bcc-tools`。**渲染与 web 服务已编进 cb，无需 perl / python3 / 任何外部脚本**——
把 `build/bin/cb` 单文件拷到目标机、装好需要的工具即可。

**你显式请求某个模式（`--profile_mode`）但它依赖的工具没装时，cb 会在启动时直接报错退出并提示 `dnf install xxx`**（不再静默跳过），例如：
`profile_mode=off_cpu needs bcc offcputime -- install it: dnf install bcc-tools`。

**注意**：

- **lock 火焰图只覆盖 mutex**（bthread/pthread），不含条件变量/信号量/自旋；采样上限 ~1000/s，绝对值为**估计**；
  最适合 `store`/`client`（真有缓存锁，如 `MemCache::Load`）。`aio` 的 `Aio::Wait` 用条件变量、**不**计入。
- web 服务绑 `0.0.0.0`，直接打印机器 IP；远程压测机用浏览器即可访问（注意端口暴露在内网）。
  服务在前台运行，**Ctrl-C 结束**。
- `--flamegraph` 会**轻微扰动吞吐**（DWARF 栈采样 + eBPF sched 探针），是「找瓶颈模式」，
  不是「跑漂亮数字模式」；要 headline 数字时别开。
- 默认输出在 `/tmp`，**不要**把 `--profile_dir` 指到被测的 `--dir` 下（`perf.data` 较大，会污染被测盘 IO）。
- 容器内即便 root，若缺 `CAP_PERFMON`/`CAP_BPF` 或 BTF，`perf`/`offcputime` 仍可能失败 —— 会自动降级。

## 代码结构

```
bench/
  main.cc            子命令分发器
  common/            跨子命令共享：flags 解析 / 格式化 / 直方图 / 统计 / 控制台报告 / 火焰图采集
  aio/ fs/ store/ client/ fsop/
                     每层独立目录，各含 options / runner / command
```

`common/` 抹平了 5 个工具原本重复的延迟直方图、分片统计和报告输出；各子命令只保留自己的 Options 与 Runner，互不耦合。
flag 解析使用自带的 `FlagSet`（非全局 gflags），从而让多个子命令同名选项（如 `--dir`、`--iodepth`）共存于同一个二进制而不冲突。
