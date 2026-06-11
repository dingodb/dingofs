---
date: 2026-06-11
topic: mdtest-bench
---

# mdtest_bench：元数据并发创建性能基准工具

## Problem Frame

需要评估 DingoFS 元数据路径（VFS → MDS）在并发创建目录/文件场景下的性能。现有 `sdk/bench/read_bench.cc`、`sdk/bench/write_bench.cc` 只覆盖数据路径。通过 libdingofs C API 直接调用 VFS 层（绕过 FUSE、无需挂载），实现类 mdtest 的元数据基准工具，落在 `sdk/bench/mdtest_bench.cc`（文件已存在但为空，CMake 尚无对应 target）。

## Requirements

**命令行参数**
- R1. 必选参数：MDS 地址（`--bench_mds_addr`）、文件系统名（`--bench_fs_name`）；测试参数：测试目录（`--bench_dir`）、并发线程数（`--bench_threads`，默认 1）、目录深度（`--bench_depth`，默认 3，须 ≥1）、目录宽度（`--bench_width`，每级分支数，默认 4，须 ≥1）、文件个数（`--bench_files`，每个叶子目录下的文件数，默认 10，mdtest `-I -L` 组合语义）、并发模式（`--bench_mode=unique|shared`，默认 `unique`）。非法的 depth/width 取值以 usage 错误拒绝。
- R2. 参数命名与风格沿用 read_bench/write_bench 的 `bench_` 前缀 gflags 约定，并保留 mount_point、conf、log_dir、log_level 等通用辅助参数（`--bench_mount_point` 为 `dingofs_mount()` 的逻辑挂载标签，与 read_bench/write_bench 一致）。

**目录树与文件布局**
- R3. 每线程（或共享模式下全局）构建 depth × width 的递归目录树：每个目录有 width 个子目录，共 depth 层。目录逐个用 `dingofs_mkdir` 创建（每目录一次调用，父目录先于子目录）；不得使用 `dingofs_mkdirs` 合并创建，否则会低估目录 ops/s。
- R4. 文件只在叶子目录创建（mdtest `-L` 语义），每个叶子目录创建"文件个数"个空文件。
- R5. 文件创建方式为 `open(O_CREAT|O_WRONLY|O_EXCL)` + `close`，不写数据，纯元数据负载。

**并发模型（参数切换两种模式）**
- R6. unique 模式（`--bench_mode=unique`，默认）：每线程在独立子树 `<bench_dir>/thread_<i>/` 下工作，互不干扰，对应 mdtest 默认行为。
- R7. shared 模式（`--bench_mode=shared`）：预先生成全部待创建路径，多线程从共享任务队列领取创建任务（mkdir 任务与文件创建任务都进入队列），测同目录并发冲突场景。冲突代价通过与 unique 模式同参数的 ops/s 对比体现（经代码验证：MDS 事务写冲突在客户端 RPC 层内部重试吸收，mkdir 为盲写不返回 EEXIST，C API 层无独立冲突信号可计数）。
- R7a. 重跑保护：启动时若发现 `<bench_dir>` 下已有上次运行的残留树，报错退出并提示使用 `--bench_force`；指定 `--bench_force` 时先删除残留（复用 R9 的后序清理逻辑）再开始测试。

**统计与输出**
- R8. 只统计创建阶段：目录创建与文件创建分阶段计时，分别输出 ops/s、总操作数、耗时；并输出汇总。所有工作线程经 barrier 同步后才开始本阶段计时；阶段耗时为 barrier 释放到最后一个线程完成的墙钟时间，ops/s = 总操作数 / 墙钟耗时。
- R9. 删除（cleanup）仅作为可选收尾步骤（`--bench_cleanup`，默认开启），不计入统计。cleanup 必须后序遍历：先逐个 `dingofs_unlink` 叶子文件，再自底向上 `dingofs_rmdir`（API 仅能删除空目录、无递归删除，否则返回 -ENOTEMPTY）。

**工程集成**
- R10. 在 `sdk/bench/CMakeLists.txt` 中新增 `mdtest_bench` target，链接方式与 read_bench/write_bench 一致（dingofs_c、gflags::gflags、glog::glog）。
- R11. 出错时记录错误并使对应线程终止，最终汇总报告错误数；非 0 退出码表示基准未完整完成。

## Success Criteria

- 不挂载 FUSE 即可对指定 MDS/文件系统跑通目录+文件创建基准并输出 ops/s。
- depth/width/files/threads 参数组合下创建的目录数与文件数符合公式预期（depth≥1、width≥1）：
  - 单棵子树：目录数 = width^1+…+width^depth，叶子数 = width^depth，文件数 = width^depth × files。
  - unique 模式总量：上述各项 × threads，另加每线程根目录 `thread_<i>`（共 threads 个）。
  - shared 模式总量：单棵子树量（全局一棵树）。
- 两种并发模式均可运行且结果可区分。
- 构建产物 `mdtest_bench` 随现有 cmake 流程编译通过。

## Scope Boundaries

- 不实现 stat / readdir / removal 阶段的性能统计（删除只是 cleanup）。
- 不支持多进程/MPI 分布式协同（mdtest 的 MPI 能力不在范围内）。
- 不写文件数据、不测数据路径。
- 不做迭代多轮（iterations）与统计学聚合（中位数/方差），单轮为准。

## Key Decisions

- 只测创建阶段：用户明确目标是并发创建性能；stat/removal 留待未来扩展。
- 文件数语义 = 每叶子目录文件数（对应 mdtest `-I` 与 `-L` 组合语义）：与 mdtest `-L` 模式习惯一致，便于对照。
- 只在叶子目录放文件（mdtest `-L`）：简化树形布局与统计。
- 空文件 open+close：隔离元数据负载，避免数据路径干扰。
- 双并发模式：unique 测扩展性，shared 测锁/事务冲突，参数切换。
- shared 模式冲突指标 = unique vs shared 同参数 ops/s 对比：规划期代码验证表明事务冲突被客户端内部重试吸收、mkdir 无 EEXIST 返回，离散冲突计数不可行。
- 重跑保护采用预检 + `--bench_force`：默认不破坏已有数据，显式授权才清理。
- 默认参数 depth=3 / width=4 / files=10 / threads=1：单线程 84 目录 + 640 文件，轻量可快速验证。

## Dependencies / Assumptions

- libdingofs C API 已提供全部所需操作：`dingofs_mkdir/mkdirs/rmdir/unlink/open/close`（已在 `sdk/c/libdingofs.h` 验证存在）。
- 多线程共用同一个 mount handle（`uintptr_t h`），C API 声明线程安全。

## Outstanding Questions

### Deferred to Planning（规划期已解决，见 docs/plans/2026-06-11-001-feat-mdtest-bench-plan.md）
- [Affects R8][Technical] 进度显示：已决定复用 read_bench 的 ProgressTracker 模式改为按 ops 计数。
- [Affects R5][已验证] VFS 不强制 O_EXCL 语义（MDS create 为盲写）；残留检测依赖 stat 预检（R7a），不依赖 open 返回 EEXIST。
- [Affects R7][已验证] 事务写冲突无独立 errno（客户端 RPC 层内部重试吸收）；冲突指标改为模式间 ops/s 对比。

## Next Steps
-> /ce:plan for structured implementation planning
