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
# 1) 裸盘极限：4K 随机读，128 队列深度，跑 30s
cb aio --dir=/data/t --rw=randread --bs=4KiB --iodepth=128 --runtime=30

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

## 代码结构

```
bench/
  main.cc            子命令分发器
  common/            跨子命令共享：flags 解析 / 格式化 / 直方图 / 统计 / 控制台报告
  aio/ fs/ store/ client/ fsop/
                     每层独立目录，各含 options / runner / command
```

`common/` 抹平了 5 个工具原本重复的延迟直方图、分片统计和报告输出；各子命令只保留自己的 Options 与 Runner，互不耦合。
flag 解析使用自带的 `FlagSet`（非全局 gflags），从而让多个子命令同名选项（如 `--dir`、`--iodepth`）共存于同一个二进制而不冲突。
