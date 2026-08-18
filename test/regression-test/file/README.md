# DingoFS 文件操作测试用例

针对 DingoFS 挂载点的文件操作测试集，共 64 个用例，每个用例一个独立 Python 脚本。

## 文件结构

- `common.py` — 公共辅助模块（测试目录解析、随机数据、md5 校验、Checker、run_case 框架）
- `test_01_*.py` ~ `test_63_*.py` — 每个用例一个独立脚本
- `run_all.py` — 批量运行器，汇总 PASS/FAIL 报表

## 使用方式

```bash
# 单个用例（在 DingoFS 挂载点上）
python3 test_39_concurrent_append.py /mnt/dingofs/testdir

# 全部运行
python3 run_all.py /mnt/dingofs/testdir

# 跳过慢用例（05 大文件 / 27 万文件 / 51 混沌 / 52 压测 / 53 并发 O_TRUNC 重写）
python3 run_all.py /mnt/dingofs/testdir --skip-slow

# 只跑指定用例
python3 run_all.py /mnt/dingofs/testdir --only 05,37,39
```

测试根目录解析优先级：

1. 第一个命令行位置参数
2. 环境变量 `DINGOFS_TEST_DIR`
3. 当前目录下 `./dingofs_test`

## 环境变量

| 变量 | 默认值 | 作用 |
|---|---|---|
| `DINGOFS_TEST_DIR` | `./dingofs_test` | 测试根目录（无位置参数时生效） |
| `DINGOFS_LARGE_SIZE_MB` | 1024 | 用例 05 大文件大小（MB） |
| `DINGOFS_MANY_FILES` | 10000 | 用例 27 目录内文件数 |
| `DINGOFS_CONC` | 8/6/4（视用例） | 并发用例的进程/线程数 |
| `DINGOFS_CHAOS_OPS` | 2000 | 用例 51 混沌测试操作次数 |
| `DINGOFS_CHAOS_SEED` | 51 | 用例 51 随机种子（复现问题用） |
| `DINGOFS_STRESS_MB` | 128 | 用例 52 每个 worker 大文件大小（MB） |
| `DINGOFS_OTRUNC_ROUNDS` | 3000 | 用例 53 每个进程 O_TRUNC 重写轮数 |

## 行为说明

- 每个用例在测试根目录下创建独立的临时子目录运行
- 用例**失败时保留**其测试目录便于排查，**通过则自动清理**
- 硬链接（24）、软链接（25）、mmap（36）在文件系统不支持时自动 SKIP 而非失败
- chown（19）非 root 运行时自动跳过
- 退出码：0 = 通过，1 = 失败，便于 CI 集成

## 用例列表

| 编号 | 类别 | 说明 |
|---|---|---|
| 01 | 基础读写 | 写入后读取 md5 一致性 |
| 02 | 基础读写 | 空文件创建与读取 |
| 03 | 基础读写 | 小文件读写（1B~几百 B） |
| 04 | 基础读写 | 块边界大小读写（4K/64K/1M/4M ± 1） |
| 05 | 基础读写 | 大文件分块读写校验（默认 1GB，slow） |
| 06 | 基础读写 | 多次 append 内容与顺序 |
| 07 | 基础读写 | 中间 offset 覆盖写，前后区域不变 |
| 08 | 基础读写 | 稀疏文件，空洞读出全 0 |
| 09 | I/O 模式 | 随机 offset pread 校验 |
| 10 | I/O 模式 | 随机 offset pwrite 后整体校验 |
| 11 | I/O 模式 | 同一 fd 交替读写 |
| 12 | I/O 模式 | 高频小写入（5000 次 1~64B） |
| 13 | 截断 | truncate 缩小 |
| 14 | 截断 | truncate 扩大，扩展区为 0 |
| 15 | 截断 | truncate 到 0 后重写 |
| 16 | 截断 | 打开中 fd 的 ftruncate 后继续读写 |
| 17 | 截断 | truncate 后 size/mtime 元数据 |
| 18 | 元数据 | 写入后 stat size/mtime/ctime 更新 |
| 19 | 元数据 | chmod / chown（非 root 跳过 chown） |
| 20 | 元数据 | utime 设置时间戳 |
| 21 | 元数据 | rename 文件 |
| 22 | 元数据 | rename 覆盖已存在目标 |
| 23 | 元数据 | unlink 后打开中 fd 仍可读写 |
| 24 | 元数据 | 硬链接（不支持则 SKIP） |
| 25 | 元数据 | 软链接（不支持则 SKIP） |
| 26 | 目录 | mkdir/rmdir，非空目录 ENOTEMPTY |
| 27 | 目录 | 大量文件 listdir 完整性（默认 1 万，slow） |
| 28 | 目录 | 深层嵌套目录（50 层） |
| 29 | 目录 | 含子文件的目录 rename |
| 30 | 目录 | 特殊文件名（中文/空格/255 字节/特殊字符） |
| 31 | 打开模式 | O_CREAT\|O_EXCL 已存在时 EEXIST |
| 32 | 打开模式 | O_TRUNC 打开清空文件 |
| 33 | 打开模式 | O_APPEND seek 后仍写到文件尾 |
| 34 | 打开模式 | 多 fd 读写可见性 |
| 35 | 打开模式 | fsync 持久性，重开校验 |
| 36 | 打开模式 | mmap 读写（不支持则 SKIP） |
| 37 | 并发 | 多进程并发写不同文件 |
| 38 | 并发 | 多线程 pwrite 同文件不同区域 |
| 39 | 并发 | 多进程并发 O_APPEND，无撕裂记录 |
| 40 | 并发 | 一写一读并发，最终一致 |
| 41 | 并发 | 并发创建/删除大量文件 |
| 42 | 并发 | rename 竞争，恰好一个成功 |
| 43 | 并发 | 并发 mkdir/rmdir 同一路径 |
| 44 | 并发 | truncate 与写入混合竞争，状态自洽 |
| 45 | 边界异常 | 读超 EOF 返回空/部分 |
| 46 | 边界异常 | 对目录 open(O_WRONLY) 报 EISDIR |
| 47 | 边界异常 | 不存在文件 open 报 ENOENT |
| 48 | 边界异常 | 路径分量为文件报 ENOTDIR |
| 49 | 边界异常 | 超长文件名报 ENAMETOOLONG |
| 50 | 边界异常 | statvfs 空间统计合理性 |
| 51 | 压力 | 混沌测试：随机操作 + 内存期望状态比对（slow） |
| 52 | 压力 | 大文件 + 小文件 + 元数据混合并发压力（slow） |
| 53 | 并发 | 多进程并发 O_TRUNC 重写同一文件（FATAL 复现，slow） |
| 54 | 打开模式 | open 各种模式：r / a / w / x / t / b |
| 55 | 并发 | 读线程并发下另一线程写（追加写 + 截断写） |
| 56 | 并发 | 读线程并发下另一线程截断（不同偏移） |
| 57 | 并发 | 读线程并发下另一线程 fallocate（不同模式） |
| 58 | 并发 | 写线程并发下另一线程截断（不同偏移） |
| 59 | 并发 | 写线程并发下另一线程写（追加写 + 截断写） |
| 60 | 并发 | 写线程并发下另一线程 fallocate（不同模式） |
| 61 | 并发 | 多线程并发读/写（追加+截断）/截断（不同偏移） |
| 62 | 并发 | 多线程并发读/写（追加+截断）/截断（不同偏移）/fallocate |
| 63 | fallocate | 基础分配、KEEP_SIZE、PUNCH_HOLE、ZERO_RANGE、边界、错误参数和 fd 语义 |
| 64 | 文件操作 | copy_file_range：跨文件/同文件、偏移、EOF、稀疏文件、边界、错误参数、隔离和并发 |
