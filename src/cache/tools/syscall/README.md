# syscall — 系统调用 / 内核原语微基准

一组**自包含**的小程序，用来量核心系统调用 / 内核原语的真实开销，帮助解释和定位性能瓶颈。
每个都只依赖 libc + pthread，既能随仓库编，也能单文件 `g++` 直接编。

| 程序 | 量什么 |
|------|--------|
| `futex_wake_bench` | 用 `futex(FUTEX_WAKE)` **唤醒一个睡着的线程**要多少纳秒 |

---

## futex_wake_bench

### 背景 / 为什么要它

DingoFS 的 `AioQueue`（`src/cache/local/aio_queue.cc`）每收到一个 IO 完成，就由**单个** `BackgroundWait`
线程执行该 aio 的 closure `aio->Run()`，通过 `bthread::ConditionVariable`（butex → futex）**唤醒一个挂起的
worker**。这次"唤醒睡着的线程"要走**内核上下文切换**，是 `cb aio` 小块高 IOPS 卡在 ~330k、加多少 `--threads`
都上不去的根因（见 [`../bench/README.md`](../bench/README.md) 的火焰图分析）。

这个工具把该开销**拆成 4 层**量出来，一眼看出"贵的是上下文切换，不是 syscall 本身"：

| 层级 | 含义 |
|------|------|
| (0) 裸 syscall（`getpid`） | 系统调用进出的地板 |
| (1) `FUTEX_WAKE` 但没人等 | 只做 futex 记账，**不唤醒任何线程** |
| (2) 唤醒睡着的线程，**同核** | + 1 次上下文切换（无跨核 IPI） |
| (3) 唤醒睡着的线程，**跨核** | + IPI + 远端 CPU 重新调度 ← **AioQueue 的情形** |

### 编译

```bash
# 随仓库一起编
make futex_wake_bench          # 产物: build/bin/futex_wake_bench

# 或单文件独立编（无任何仓库依赖）
g++ -O2 -pthread futex_wake_bench.cc -o futex_wake_bench
```

### 运行 & 输出示例

无需 root。直接跑，无参数：

```
$ ./futex_wake_bench
futex wake microbenchmark  (online cpus = 128)

(0) raw syscall floor (getpid)                  :   110 ns
(1) FUTEX_WAKE, no waiter                        :   129 ns
(2) wake a SLEEPING thread, same core (cpu0)     :  1163 ns  (one-way)
(3) wake a SLEEPING thread, cross core (cpu0->2) :  2527 ns  (one-way)

(3) is what DingoFS AioQueue pays per completion ...
```
（上面是 jg30 / Intel Xeon 的实测值，仅供参考——**结果与 CPU 型号、内核、拓扑、负载相关**。）

### 怎么读

- **(0) ≈ (1)**：`FUTEX_WAKE` 这个 syscall 本身很便宜（~130ns），跟裸 syscall 几乎一样——**没人等时它不唤醒任何线程**。
- **(2)、(3) 暴涨到 µs 级**：一旦真的要**把一个睡着的线程调度上 CPU**，就是一次内核上下文切换，跳到
  ~1.2µs（同核）/ ~2.5µs（跨核），是裸 syscall 的 **10~20 倍**。
- 所以性能瓶颈里"每 IO 一次 futex 唤醒"贵在**上下文切换**，不在 syscall。

### 和 AioQueue 330k 上限的关系

(3) 的 ~2.5µs 就是 `AioQueue` 单个 `BackgroundWait` 线程**每个完成跨核唤醒一个 worker 的成本**：

```
1 / 2.5µs ≈ 396k ops/s      # 纯唤醒的单线程上限
```

`cb aio` 实测 ~330k，略低于此（每完成还要 `WaitIO` 收割 + `OnComplete` + `bthread::Mutex` + butex 层开销）。
**数字对得上**——这就是"一个线程 × 每完成一次 ~2.5µs 跨核唤醒"的天花板。要突破得给 `AioQueue`
分片（多 io_uring + 多完成线程）或让 worker 自己收割完成（免跨线程唤醒）。

### 实现原理（一句话）

两个线程**严格交替**：各自 `FUTEX_WAIT` 睡下、被对方 `FUTEX_WAKE` 唤醒，所以**每次唤醒都命中一个真正睡着的线程**。
一个来回 = 2 次唤醒，`总时间 / (2N)` = 单向唤醒延迟。用 `sched_setaffinity` 把两线程绑到同核 / 跨核，分别得到 (2)/(3)。
