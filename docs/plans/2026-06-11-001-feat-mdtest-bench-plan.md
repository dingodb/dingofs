---
title: "feat: Add mdtest_bench metadata creation benchmark"
type: feat
status: active
date: 2026-06-11
origin: docs/brainstorms/2026-06-11-mdtest-bench-requirements.md
---

# feat: Add mdtest_bench metadata creation benchmark

## Overview

新增 `sdk/bench/mdtest_bench.cc`：类 mdtest 的元数据并发创建基准工具，通过 libdingofs C API 直接调用 VFS 层（绕过 FUSE、无需挂载），分阶段统计目录创建与文件创建的 ops/s。支持 unique（每线程独立子树）与 shared（共享任务队列、测同目录并发冲突）两种并发模式。

## Problem Frame

现有 `sdk/bench/read_bench.cc`、`sdk/bench/write_bench.cc` 只覆盖数据路径，元数据路径（VFS → MDS）的并发创建性能无工具可测（see origin: docs/brainstorms/2026-06-11-mdtest-bench-requirements.md）。

## Requirements Trace

来自 origin 文档（R1–R11，详见原文）：

- R1/R2. gflags 参数：`bench_mds_addr`、`bench_fs_name`、`bench_dir`、`bench_threads`(=1)、`bench_depth`(=3,≥1)、`bench_width`(=4,≥1)、`bench_files`(=10)、`bench_mode`(=unique, unique|shared)、`bench_mount_point`、`bench_conf`、`bench_log_dir`、`bench_log_level`、`bench_cleanup`(=true)、`bench_force`(=false)。
- R3. depth×width 递归目录树，逐个 `dingofs_mkdir`（父先于子），禁用 `dingofs_mkdirs`。
- R4/R5. 仅叶子目录创建空文件：`dingofs_open(O_CREAT|O_WRONLY|O_EXCL)` + `dingofs_close`。
- R6. unique 模式：每线程独立子树 `<bench_dir>/thread_<i>/`。
- R7. shared 模式：mkdir+文件创建任务共享队列；冲突代价通过与 unique 模式同参数 ops/s 对比体现（无离散冲突计数，见 Key Technical Decisions）。
- R7a. 所有模式：启动预检残留树，`--bench_force` 先清理（→ Unit 4）。
- R8. 分阶段（目录/文件）barrier 同步计时，输出 ops/s、总操作数、墙钟耗时及汇总。
- R9. cleanup 后序遍历（先 unlink 后自底向上 rmdir），不计入统计。
- R10. CMake 新增 `mdtest_bench` target（dingofs_c、gflags::gflags、glog::glog）。
- R11. 错误终止对应线程、汇总错误数、非 0 退出码。

## Scope Boundaries

- 不统计 stat / readdir / removal 阶段性能（删除仅 cleanup）。
- 不支持多进程/MPI 协同。
- 不写文件数据、不测数据路径。
- 不做多轮迭代与统计学聚合（中位数/方差）。

## Context & Research

### Relevant Code and Patterns

- `sdk/bench/read_bench.cc` — 模板：flag 定义风格（66–104 行）、`NowSec()`/`HumanSize()` 辅助函数、`ProgressTracker`（145–197 行，按 bytes，需改按 ops）、mount 流程（dingofs_new → conf_set("log.dir"/"log.level") → 可选 conf_load → dingofs_mount）、线程 launch/join、结果表打印、cleanup → umount → delete。
- `sdk/bench/write_bench.cc:401-402`（注释）+ `403-408`（实现）— "Pre-create bench directory and per-thread subdirectories serially to avoid MDS TxnWriteConflict on the parent inode"，印证 shared 模式并发 mkdir 确会触发冲突路径。
- `sdk/bench/CMakeLists.txt` — target 定义模式（add_executable + include sdk/ + link dingofs_c, gflags::gflags, glog::glog）。
- `sdk/c/libdingofs.h` — 所需 API 齐备：`dingofs_mkdir/rmdir/unlink/open/close/stat/opendir/readdir/closedir/mount/umount`。

### Institutional Learnings

- 无 `docs/solutions/` 目录，无相关沉淀。

### 规划期已验证的技术事实

- **MDS create/mkdir 为盲写、不返回 EEXIST**：`src/mds/filesystem/store_operation.cc:714-733` `MkDirOperation::RunInBatch` 直接 `txn->Put` dentry，无存在性检查；文件 create 同理。且 `src/client/vfs/metasystem/mds/rpc.h:187-211` `TransformError` 无 `EEXISTED` 分支（default → Status::Internal → EIO）。**因此 O_EXCL 不提供独占保证，重复创建会静默覆盖 dentry**——残留检测必须依赖 stat 预检（Unit 4），不能依赖 open/mkdir 返回 -EEXIST。
- **写冲突 errno**：MDS 事务冲突（`ESTORE_MAYBE_RETRY`，`src/mds/storage/dingodb_storage.cc:223-248`）在客户端 RPC 层内部重试（`src/client/vfs/metasystem/mds/rpc.h:390-399`），不以独立 errno 暴露。C API 层无离散冲突信号；冲突代价只能通过吞吐/延迟观测。

## Key Technical Decisions

- **shared 模式无离散冲突计数**：经代码验证（见上节），事务冲突被 RPC 层内部重试吸收、mkdir 为盲写不返回 EEXIST，C API 层不存在可计数的冲突信号。冲突代价通过 unique vs shared 同参数 ops/s 对比体现（用户决策，origin R7 已同步更新）。
- **共享 mount handle + std::thread**：C API 声明线程安全（libdingofs.h DESIGN NOTES），与 read/write_bench 的多线程单 handle 用法一致。
- **shared 模式任务依赖顺序**：任务队列按 BFS 层序入队（先所有第 1 层 mkdir，再第 2 层……最后叶子文件），队列按阶段分批发放——同一层内并发（制造同父目录冲突），层间以 barrier 分隔，避免父目录尚不存在导致 -ENOENT 误报为错误。
- **计时按阶段而非按任务**：每阶段 barrier 同步开始，墙钟时间 = barrier 释放至最后线程完成；目录阶段与文件阶段分别计 ops/s。
- **预检用 dingofs_stat**：启动时 stat 各模式的根路径（unique：`<bench_dir>/thread_0`；shared：`<bench_dir>/shared`），存在即判定残留。

## Open Questions

### Resolved During Planning

- O_EXCL 是否被 VFS 支持：**否**——MDS create/mkdir 为盲写（无存在性检查），重复创建静默覆盖 dentry，不返回 -EEXIST。残留检测依赖 stat 预检（Unit 4）。
- 事务写冲突的 errno：无独立 errno，客户端 RPC 层内部重试吸收；离散冲突计数不可行，改用 unique vs shared 同参数 ops/s 对比（用户决策）。
- 进度显示：复用 ProgressTracker 模式改为按 ops 计数（total_ops 替代 total_bytes）。

### Deferred to Implementation

- shared 模式队列粒度（单任务领取 vs 小批量领取）：以实测队列锁是否成为瓶颈为准，默认单任务 + `std::mutex` 队列，简单优先。
- 结果表的具体列布局：参照 read_bench 的表格风格，实现时微调。

## Implementation Units

- [x] **Unit 1: 骨架——flags、mount 流程与 CMake target**

**Goal:** `mdtest_bench` 可编译、可挂载/卸载，参数解析与校验完整。

**Requirements:** R1、R2、R10

**Dependencies:** None

**Files:**
- Modify: `sdk/bench/mdtest_bench.cc`（现为空文件）
- Modify: `sdk/bench/CMakeLists.txt`

**Approach:**
- 按 read_bench.cc 的结构组织：文件头注释（用途+用法示例）、gflags 定义、`NowSec()` 辅助、main 中 mount → 跑基准 → cleanup → umount。
- 参数校验：`bench_mds_addr`/`bench_fs_name` 非空，`bench_depth≥1`、`bench_width≥1`、`bench_files≥0`、`bench_threads≥1`、`bench_mode ∈ {unique, shared}`，非法值打印 usage 并以非 0 退出。
- mount 流程复制 read_bench：`dingofs_new()` → `dingofs_conf_set("log.dir"/"log.level")` → 可选 `dingofs_conf_load(bench_conf)` → `dingofs_mount(h, addr, fs, mount_point)`，失败打印 `-errno` 并退出。
- CMake target 额外链接 `Threads::Threads`（`find_package(Threads REQUIRED)`）：现有 bench 未用 pthread_barrier，glibc<2.34 环境下隐式链接不可靠。

**Patterns to follow:**
- `sdk/bench/read_bench.cc` flag 定义与 main 骨架；`sdk/bench/CMakeLists.txt` 现有两个 target 的写法。

**Test scenarios:**
- Test expectation: none — 仓库 bench 工具（read/write_bench）无单测基础设施；本单元验证为构建通过与参数校验的手工检查（非法 depth=0 / 未知 mode 应拒绝）。

**Verification:**
- `cmake --build build --target mdtest_bench` 成功产出二进制。
- 不带必选参数运行时输出清晰 usage 错误；`--bench_depth=0` 被拒绝。

- [x] **Unit 2: 目录树模型 + unique 模式 + 分阶段统计**

**Goal:** unique 模式端到端可跑：每线程独立子树，目录/文件两阶段 barrier 计时，输出 ops/s 报告。

**Requirements:** R3、R4、R5、R6、R8、R11

**Dependencies:** Unit 1

**Files:**
- Modify: `sdk/bench/mdtest_bench.cc`

**Approach:**
- 路径生成：迭代（非递归）BFS 生成每层目录路径列表（层 d 有 width^d 个目录），叶子层附加 `file_<j>` 文件路径；目录命名 `d<level>_<index>` 风格，文件命名 `f<j>`。
- 每线程持有自己的路径集（根 `<bench_dir>/thread_<i>`，该根目录创建计入目录阶段）。
- 两阶段执行：阶段开始前 `pthread_barrier_t`（或自实现 barrier）同步；目录阶段逐个 `dingofs_mkdir`（BFS 序保证父先于子）；文件阶段 `dingofs_open(O_CREAT|O_WRONLY|O_EXCL)`+`dingofs_close`。
- 统计：每线程记录 ops 数与错误数；阶段墙钟时间由主线程在 barrier 释放与全部 join 后测量；ops/s = Σops / 墙钟，须防除零（`elapsed > 1e-9 && total_ops > 0` 才计算，否则输出 0/N/A）。汇总表 + 总计行。
- ProgressTracker 改造：按 ops 计数（atomic<size_t> ops / total_ops），1 秒刷新。
- 错误处理（R11）：任一调用返回负值即记录（路径、errno）、线程置错误标志并退出循环；main 汇总错误数，>0 时退出码非 0。

**Patterns to follow:**
- `sdk/bench/read_bench.cc` 的 ThreadResult / 线程函数 / 结果表打印；`HumanSize` 不需要，改为 ops 数字输出。

**Test scenarios:**
- Test expectation: none — 无 bench 单测设施。手工验证场景（对 dummy/开发 MDS 运行）：
  - Happy path：`--bench_threads=2 --bench_depth=2 --bench_width=3 --bench_files=5` → 报告目录数 = 2×(3+9)+2(线程根)=26，文件数 = 2×9×5=90，与 Success Criteria 公式一致。（注：origin Key Decisions 中默认参数"84 目录"未计线程根目录，实际默认运行报告 85；以 Success Criteria 公式为准。）
  - Edge case：`--bench_files=0` → 文件阶段 0 ops，不除零（ops/s 输出 0 或 N/A）。
  - Edge case：`--bench_width=1 --bench_depth=5` → 线性链，正常完成。
  - Error path：构造一个 mkdir 必然失败的场景（如 bench_dir 指向只读/不存在的父路径）→ 错误被记录、线程终止、退出码非 0。（注：对残留树重跑不会报错——MDS 盲写静默覆盖，这正是 Unit 4 预检存在的原因。）

**Verification:**
- unique 模式运行输出两阶段 ops/s 与汇总，目录/文件计数符合公式（见 origin Success Criteria）。

- [x] **Unit 3: shared 模式——共享任务队列**

**Goal:** shared 模式可跑：全局一棵树，线程从共享队列领任务，层间 barrier 保证父目录先于子目录。

**Requirements:** R7、R8、R11

**Dependencies:** Unit 2

**Files:**
- Modify: `sdk/bench/mdtest_bench.cc`

**Approach:**
- 全局树根 `<bench_dir>/shared`（主线程串行创建根，不计时——与 write_bench 注释的做法一致）。
- 任务按层分批：第 d 层全部 mkdir 任务装入 `std::mutex` 保护的队列（或带原子游标的 vector），全部线程消费完毕后 barrier，再进入第 d+1 层；最后一批为叶子文件创建任务。层内多线程并发对同父目录 mkdir，在 MDS 侧制造真实事务冲突（被客户端内部重试吸收，体现为延迟/吞吐变化）。
- 错误语义与 unique 模式一致（R11）：任何负返回值均为 error。无离散冲突计数（见 Key Technical Decisions）；冲突代价由 unique vs shared 同参数 ops/s 对比体现。
- 计时仍分目录阶段（所有层合计）与文件阶段。

**Technical design:** *(directional guidance, not implementation specification)*
```
for level in 1..depth:        # 目录阶段（整体计时）
    queue = all mkdir tasks of this level
    barrier; threads drain queue; barrier
file phase: queue = all leaf-file create tasks; barrier; drain; barrier
```

**Patterns to follow:**
- `sdk/bench/write_bench.cc:401-402`（注释）+ `403-408`（实现）：串行预建根目录的做法（仅用于不计时的根）。

**Test scenarios:**
- Test expectation: none — 无 bench 单测设施。手工验证场景：
  - Happy path：`--bench_mode=shared --bench_threads=8 --bench_depth=2 --bench_width=4 --bench_files=10` → 目录数 = 4+16=20（不乘线程数），文件数 = 160；退出码 0。
  - Integration：threads=8 > 第 1 层任务数 4 时，部分线程空转直达 barrier——验证无死锁、无 -ENOENT（层间 barrier 生效）。
  - Edge case：`--bench_threads=1` shared 模式 → 结果与公式一致。
  - Integration：同参数分别跑 unique 与 shared，对比 ops/s 可观测差异（shared 因同父事务冲突重试而吞吐更低）。

**Verification:**
- shared 模式完成后目录/文件总数与单棵树公式一致；退出码为 0；unique vs shared 对比能体现冲突开销。

- [x] **Unit 4: 重跑预检、--bench_force 与后序 cleanup**

**Goal:** 工具可安全重复运行：预检残留、强制清理、默认 cleanup 后序删除整棵树。

**Requirements:** R7a、R9

**Dependencies:** Unit 2、Unit 3

**Files:**
- Modify: `sdk/bench/mdtest_bench.cc`

**Approach:**
- 预检：mount 后、计时前，`dingofs_stat` 探测本次运行将创建的根（unique：`thread_0`；shared：`shared`）。存在且无 `--bench_force` → 打印明确错误（提示加 `--bench_force`）并以非 0 退出；有 `--bench_force` → 先执行清理再继续。
- 清理实现 `RemoveTreeRecursive(h, path)`：`dingofs_opendir/readdir` 枚举，DT_DIR 递归后 `dingofs_rmdir`，其余 `dingofs_unlink`——后序保证 rmdir 时目录已空。基于 readdir 而非路径公式，确保能清掉任意残留（含上次部分失败的树）。
- cleanup（R9）：基准完成后默认对各根执行 RemoveTreeRecursive，不计时；`--bench_cleanup=false` 跳过并提示路径。
- cleanup 失败仅告警（不影响基准统计的退出码语义，但若基准本身有错误仍非 0）。

**Patterns to follow:**
- `sdk/c/libdingofs.h` 的 opendir/readdir/closedir 用法约定（dirent 类型 DT_DIR/DT_REG）。

**Test scenarios:**
- Test expectation: none — 无 bench 单测设施。手工验证场景：
  - Happy path：连续两次运行（默认 cleanup 开）→ 第二次预检通过、正常完成。
  - Error path：第一次 `--bench_cleanup=false`，第二次默认参数 → 预检报错退出、提示 --bench_force。
  - Happy path：第三次加 `--bench_force` → 清掉残留后正常完成。
  - Edge case：对深树（depth=4, width=4）cleanup → 全部删除，再次 stat 根返回 -ENOENT。

**Verification:**
- 上述三连跑行为符合预期；cleanup 后文件系统无残留。

## System-Wide Impact

- **Interaction graph:** 纯新增工具二进制，不修改 client/MDS 任何代码；仅消费 libdingofs C API 公开接口。
- **Error propagation:** C API 负 errno → 工具内分类（EEXIST-as-conflict 仅限 shared 模式 mkdir）→ 汇总与退出码。
- **State lifecycle risks:** 中途崩溃会在目标文件系统留下残留树——由 Unit 4 预检 + --bench_force 缓解。
- **API surface parity:** 无；不改动任何公共接口。
- **Unchanged invariants:** read_bench/write_bench 及其 CMake target 不受影响。

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| shared 模式队列锁本身成为瓶颈，测的是队列而非 MDS | 队列操作为 O(1) 弹出，任务执行含网络 RPC（毫秒级），锁占比可忽略；如实测异常，改原子游标方案（已列为 deferred） |
| width^depth 组合爆炸（如 depth=6,width=10）压垮客户端/MDS | 启动时打印将创建的目录/文件总数；总数超阈值（如 10^7）时要求显式确认参数合理（打印警告） |
| RPC 层内部重试吸收冲突，shared 模式冲突不可直接观测 | 已确认（规划期验证）；以 unique vs shared 同参数 ops/s 对比作为冲突代价指标，报告中注明语义 |
| 客户端 dentry 缓存使 ops/s 偏乐观（相对冷缓存） | 接受：本工具测的是客户端视角的创建吞吐；文档头注释中说明 |

## Sources & References

- **Origin document:** [docs/brainstorms/2026-06-11-mdtest-bench-requirements.md](../brainstorms/2026-06-11-mdtest-bench-requirements.md)
- Related code: `sdk/bench/read_bench.cc`、`sdk/bench/write_bench.cc`、`sdk/c/libdingofs.h`、`sdk/c/libdingofs.cc`
- 冲突路径佐证: `src/client/vfs/metasystem/mds/rpc.h`（ESTORE_MAYBE_RETRY 内部重试）、`src/mds/storage/dingodb_storage.cc`（冲突映射）
