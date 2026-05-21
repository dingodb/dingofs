# DingoFS Connector for LMCache

把 vLLM 生态里的 [LMCache](https://github.com/LMCache/LMCache) 接到一个 dingo-cache 集群，让 KV cache 走 dingofs 的分布式缓存。

同时实现了 LMCache 的两套接口：

| 接口 | 类 | 用途 |
|---|---|---|
| `RemoteConnector` | `DingoFSConnector` | 单远端缓存层（按 URL 寻址：`dingofs://...`） |
| `L2AdapterInterface` | `DingoFSL2Adapter` | 分布式存储层（batched + eventfd 调度） |

设计要点：

- **零拷贝 Put**：Python `memoryview` 通过 `butil::IOBuf::AppendUserData` 直接挂载到 brpc，发送路径无客户端拷贝。
- **端到端 1 次拷贝 Get**：响应 IOBuf 通过 `cutn(dst, len)` 直写 caller buffer —— brpc 框架下的物理下限（等价 mooncake/infinistore）。
- **进程内 LRU 短路 Exists**：put / get 命中即在内存 set 上加键，prefetch 阶段的 batched_async_contains 大部分调用 0 syscall。
- **pybind11 + GIL 释放**：异步提交全程释放 GIL；bthread 完成回调通过 eventfd 唤醒 demux 线程，再 `loop.call_soon_threadsafe` 跳回 asyncio。
- **CPU 内存池透传**：connector 启动时从 LMCache `LocalCPUBackend` 的 pinned arena 提取 `(addr, length)`，传给 native 留作 RDMA 注册之用。Put / Get 数据都是该 arena 的切片，**注册一次覆盖所有数据流**。当前只存地址，`ibv_reg_mr` 留给后续 RDMA PR。

## 前置依赖

- 一个跑着的 `dingo-mds` + `dingo-cache` 集群
- Python ≥ 3.9
- LMCache（确保 `lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter` 可 import）
- 编译 dingofs 所需的全部三方库（参考 dingofs 主仓 README）

## 编译与安装

connector 的原生扩展（`_dingofs_native.so`）由 dingofs 主 CMakeLists 编译，通过开关 `BUILD_LMCACHE_CONNECTOR` 挂载，**默认 OFF**，不影响其他构建。

首次构建前确保子模块拉好：

```bash
cd dingofs
git submodule sync && git submodule update --init --recursive
```

然后一条命令出 wheel：

```bash
cd integration/lmcache
make
# 产出：dist/dingofs_connector-0.1.0-cp<ver>-cp<ver>-linux_x86_64.whl
```

`make` 做的事：cmake configure（若 `build/` 没配置过）→ `make _dingofs_native` → `strip --strip-debug` → `pip wheel` 到 `./dist/`。

常用变体：

```bash
make native            # 只编 .so，不 strip 不打 wheel（开发用，配合 pip install -e .）
make clean             # 清掉 .so 和 dist/
make JOBS=8            # 覆盖并发数（默认 nproc）
make BUILD_TYPE=Release

# 对真集群跑一遍 ping + roundtrip 烟测
make test REMOTE_URL=dingofs://mds1:6700,mds2:6700/lmcache_group
make test REMOTE_URL=dingofs://... HASH=0xdeadbeef   # 自定义 chunk_hash
```

开发模式安装（`.so` 已在 `src/dingofs_connector/` 下，直接 editable install）：

```bash
make native
pip install -e .
```

scp wheel 到线上机后 `pip install dingofs_connector-*.whl` 即可，不需要装 dingofs / brpc / glog（都已静态链入 `.so`）。

## 配置 LMCache

### A) 作为 RemoteConnector

LMCache **≥ 0.4.5（推荐，plugin 模式）**：

```yaml
remote_storage_plugins: ["dingofs"]
extra_config:
  # 必填：动态导入 adapter
  remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
  remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter
  # 必填：dingofs 集群地址
  remote_storage_plugin.dingofs.url:         dingofs://mds1:6700,mds2:6700/lmcache_group

  # 可选：dingofs gflags（任意条目）从这个 conf 文件加载。
  # 文件格式与 dingo-mds / dingo-client / cache-bench 完全一致，
  # 一行一个 `--flag=value`，`#` 开头注释。可用 `dingo-mds --tmpl` 生成起步模板。
  remote_storage_plugin.dingofs.conf:        /etc/dingofs/lmcache-client.conf

  # 可选：连接器单 chunk 上限（MiB）。详见下文"客户端 chunk 上限"。
  remote_storage_plugin.dingofs.max_chunk_mib: 16

# 注意：不要再设 remote_url —— v0.4.5 起 remote_url 已 deprecated，
# 且同时配 remote_storage_plugins + remote_url 会创建两个 RemoteBackend（双写）。
```

`lmcache-client.conf` 内容示例：

```
# dingofs cache RPC tuning
--cache_rpc_timeout_ms=5000
--cache_rpc_max_retry_times=3
--cache_rpc_max_timeout_ms=60000

# block cache local I/O
--iodepth=256

# logging
--log_dir=/var/log/dingofs-lmcache
```

LMCache 旧版 / legacy `remote_url` 写法（不推荐，且无法配 conf / max_chunk_mib）：

```yaml
remote_storage_plugins: ["dingofs"]
extra_config:
  remote_storage_plugin.dingofs.module_path: dingofs_connector.adapter
  remote_storage_plugin.dingofs.class_name:  DingoFSConnectorAdapter
remote_url: "dingofs://mds1:6700,mds2:6700,mds3:6700/lmcache_group"
```

### B) 作为 L2 Adapter

`l2_factory` 在包 import 时自动注册类型 `"dingofs"`，配置直接写：

```bash
--l2-adapter '{"type":"dingofs","mds_addrs":"mds1:6700,mds2:6700","cache_group":"lmcache_group"}'
```

或 YAML：

```yaml
l2_adapter:
  type: dingofs
  mds_addrs: "mds1:6700,mds2:6700"
  cache_group: "lmcache_group"
  extra:               # 可选：额外 dingofs gflag 覆盖
    cache_rpc_timeout_ms: "5000"
    cache_rpc_max_retry_times: "3"
```

## URL 语法

```
dingofs://<mds-addrs>/<cache-group>
```

URL 严格只接受这两个字段，**不支持查询字符串**（含 `?` 会直接 `ValueError`）。

- `<mds-addrs>` 单个 `host:port`，或多个逗号分隔（仿 redis-sentinel）：
  - `dingofs://mds1:6700/grp`
  - `dingofs://mds1:6700,mds2:6700,mds3:6700/grp`
- `<cache-group>` 服务端预先创建好的 cache group 名

### dingofs gflag 覆盖

在 lmcache.yaml 中指一个 conf 文件路径：

```yaml
remote_storage_plugin.dingofs.conf: /etc/dingofs/lmcache-client.conf
```

文件格式为标准 dingofs gflags conf（`--flag=value` 一行一条，`#` 开头注释），native 侧通过 [`gflags::ReadFromFlagsFile`](../../src/common/flag.cc) 加载 —— 与 `dingo-mds`、`dingo-client`、`cache-bench` 完全同套。可用 `dingo-mds --tmpl > base.conf` 生成起步模板再裁剪。

未知 flag 名会让连接器启动失败（`errors_are_fatal=true`），typo 立刻可见。

### 客户端 chunk 上限（连接器自身的 knob）

| 来源 | key | 默认 |
|---|---|---|
| yaml | `remote_storage_plugin.dingofs.max_chunk_mib` | — |
| env | `DINGOFS_MAX_CHUNK_MIB` | — |
| 内置 | — | **4** |

优先级：**env > yaml > 默认**。启动时 LMCache 推算出的 chunk 字节 > 此值会 `ValueError`。默认 4 MiB 对齐 cache 节点 `LocalFileSystem` 的 io_uring fixed buffer slot（[src/cache/blockcache/local_filesystem.cc:56-59](../../src/cache/blockcache/local_filesystem.cc#L56-L59)）。集群部署侧把 buffer slot 调大后再相应放开客户端。

## 烟测客户端（验证集群读写）

`examples/client.py` 是一个 CLI 工具，**走完整的 `DingoFSConnector` 链路**（CacheEngineKey + MemoryObj + asyncio + native module + brpc），但不需要拉起完整的 LMCacheEngine —— 仅用最小 mock 提供 `LocalCPUBackend.allocate` 等几个被实际访问的方法。

适合的场景：起一个 dingo-mds + dingo-cache 集群后，先用它验证读写通路，再去看缓存节点的磁盘上有没有数据落地。

```bash
# 0. 健康探针
python examples/client.py --url dingofs://mds1:6700/lmcache_group ping

# 1. 端到端 round-trip：put 一段 deterministic 数据 → get → 字节比对
python examples/client.py \
    --url dingofs://mds1:6700,mds2:6700/lmcache_group \
    roundtrip --hash 0xcafebabe

# 2. 拆开来验证：先 put，再去集群节点上看文件，最后从客户端 get 校验
python examples/client.py --url dingofs://... put --hash 0xdeadbeef --seed 0x1234

# 此时去任一 cache group 节点：
#   ls -lh /path/to/cache_dir/tensor/<hash[0:2]>/<hash[0:4]>/
# 应能看到 smoke-client@<ws>@<wid>@deadbeef@lmcache 这样命名的文件

python examples/client.py --url dingofs://... get --hash 0xdeadbeef --seed 0x1234 --verify

# 3. exists 探测
python examples/client.py --url dingofs://... exists --hash 0xdeadbeef
```

参数说明：
- `--hash`：模拟 LMCache 的 `chunk_hash`，决定 cache key（写出去的文件路径由它和 `--model`、`--world-size`、`--worker-id` 一起决定）
- `--seed`：deterministic payload 的种子（用 8 字节 little-endian 模式填充整个 chunk）
- `--model` / `--world-size` / `--worker-id`：构造 CacheEngineKey 时用，需要 put 和 get 用同一组才能命中

退出码：`0` 成功，非 0 失败（NotFound / verify 失败 / 网络错误）。

> 注：默认 chunk 大小 = 64 KiB（fp16，shape `[2,1,256,64]`），可在 `examples/_fake_lmcache.py` 里改 `FakeMetadata` 调。

### watch 模式 + 并发参数（模拟 LMCache 调用 pattern）

`watch` 子命令带 `--concurrency N`：每轮用 `asyncio.gather` 并发 fire N 个 `batched_*` 协程到同一个 connector loop —— **正是 LMCache fire-and-forget put / parallel batched_get 的并发形态**。

```bash
# 单协程串行（默认，等同老版本）
python examples/client.py --url dingofs://... watch \
    --op get --count 20 --size $((4*1024*1024)) --duration 30

# 4 路并发：每路 20 keys × 4MiB，loop 里 4 个 batched_get 协程同时活着，
# 4 个 native batched RPC 一起飞
python examples/client.py --url dingofs://... watch \
    --op get --count 20 --concurrency 4 --size $((4*1024*1024)) --duration 30

# 写并发：每路 20 keys 随机 hash，4 路并行 put
python examples/client.py --url dingofs://... watch \
    --op put --count 20 --concurrency 8 --size $((4*1024*1024)) --duration 30
```

**关键观察**：

- **wall 是 gather 的总等待**（即"最慢那个协程"的耗时）。并发拉满后单轮 wall 不应该明显增加，但 `round_mib = count × size × concurrency` 暴涨 → 吞吐线性增长才是健康的。
- **如果 wall 随 concurrency 线性增长** → 说明 native / 服务端串行化了请求（bthread 池打满 / 单 cache 节点串行处理同 key 等）。
- 配合 access log（`DINGOFS_ACCESS_LOG_ENABLED=1`）能直接看到多路并发的 `native.batch_get` 入口时间戳是否真的同时，以及 `native.drain_completions` 单次能否合并多个完成事件。

## Access Log

每个 LMCache 调用 connector / connector 调 native 的入口都打一行 access log，方便观察 LMCache 的调用 pattern 和各层时延。格式跟 dingofs vfs access log 一致：

```
<timestamp> <op>(<args>) : <result> <duration_seconds>
```

例：

```
2026-05-23 10:45:40.754 native.batch_set(1 keys, 65536 bytes) : ok <0.000920>
2026-05-23 10:45:40.754 put(smoke-client@01000000@a0a0a0a1, 64.00KiB) : ok <0.000999>
2026-05-23 10:46:01.689 batched_get(20 keys) : hits=20/20 <0.007733>
2026-05-23 10:46:01.689 native.batch_get(20 keys, 83886080 bytes) : hits=20/20 <0.007600>
```

两层都打：外层 `put` / `get` / `batched_*` 是 LMCache 看到的延迟；内层 `native.batch_*` 是真实 RPC 边界。差值 = Python wrapper 开销。

**默认关闭**。通过环境变量控制（启动时读一次，运行中不变）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `DINGOFS_ACCESS_LOG_ENABLED` | `0` | 设 `1` / `true` / `yes` / `on` 启用 |
| `DINGOFS_ACCESS_LOG_PATH` | 空（→ stderr） | 文件路径；空写 stderr |
| `DINGOFS_ACCESS_LOG_THRESHOLD_US` | `0` | 微秒阈值，只记录耗时 ≥ 阈值的 op；`0` 全记 |

被记录的入口（**0 漏**：RemoteConnector 基类全部 21 个公开方法 + L2AdapterInterface/NativeConnectorL2Adapter 全部 17 个公开方法 + native 调用边界）：

- **DingoFSConnector**（RemoteConnector）：
  - 启动 hook：`post_init`
  - 数据面：`exists` / `exists_sync` / `batched_async_contains` / `get` / `put` / `batched_get` / `batched_put` / `batched_get_non_blocking` / `remove_sync` / `batched_contains` / `ping` / `list` / `close`
  - 工具：`reshape_partial_chunk`（partial-read 时 LMCache storage_manager 会调；dingofs 当前总返回 full chunk，应该不会触发，触发了能从日志看到）
  - 特性探测：`support_ping` / `support_batched_get` / `support_batched_put` / `support_batched_async_contains` / `support_batched_get_non_blocking` / `support_batched_contains`
- **DingoFSL2Adapter**（NativeConnectorL2Adapter）：
  - 启动 hook：`l2.get_store_event_fd` / `l2.get_lookup_and_lock_event_fd` / `l2.get_load_event_fd` / `l2.register_listener`
  - 数据面：`l2.submit_store_task` / `l2.pop_completed_store_tasks` / `l2.pop_completed_store_task_bytes` / `l2.submit_lookup_and_lock_task` / `l2.query_lookup_and_lock_result` / `l2.submit_unlock` / `l2.submit_load_task` / `l2.query_load_result` / `l2.delete` / `l2.close`
  - 状态/可观测：`l2.get_usage`（eviction controller 周期 poll）/ `l2.report_status`
  - 特性探测：`l2.supports_global_eviction`
- **DingoFSNativeClient**（native 调用边界）：`native.batch_set` / `native.batch_get` / `native.batch_exists` / `native.exists_sync` / `native.ping_sync` / `native.drain_completions` / `native.close`

未启用时开销近 0（contextmanager 立即返回 stub，不计时不格式化 args）。

## 运行测试

```bash
cd integration/lmcache
pip install -e ".[test]"

# 纯 Python 单测（不需要集群）
pytest tests/unit

# 集成测试（需要一个跑着的 dingo-mds + dingo-cache 集群）
pytest tests/integration -m integration
```

## 运维注意

- **一个进程一个 connector 实例**：dingofs gflags 是进程全局的，pybind 层会拒绝第二次 `RemoteCache` 构造。LMCache 自己一进程一个 engine，这是天然契合。
- **dtype 不参与 server 端 key**：client 端把 dtype 信息丢弃（用占位 `"lmcache"`），server 端 path 为 `tensor/XX/XXXX/model@ws@wid@hash@lmcache`。chunk_hash 已是内容哈希，dtype 不需要再独立区分。
- **没有 list 枚举**：dingofs 缓存层无枚举 RPC。`list()` 返回空列表（用于 LMCache 诊断路径）。
- **Exists LRU 容量**：默认 100 万条 ≈ 100MB 内存。可在代码层调 `DingoFSConnector(exists_cache_capacity=...)`。LRU 容忍假阳性（远端 evict 后 Get 自然失败），不容忍假阴性 → 未命中必查远端。
- **Paged allocator 限制**：开了 LMCache `enable_p2p: true` 会用 `PagedCpuGpuMemoryAllocator`，当前 connector 暂未支持枚举其分页 —— 启动时打一行 warning 但**不阻塞**，RDMA 通路在此模式下不可用。PD 分离（`enable_pd: true`）跟 P2P 互斥，PD 用户仍是 `MixedMemoryAllocator`，走默认路径不触发 warning。

## 故障排查

| 现象 | 可能原因 |
|---|---|
| 启动报 `mds_addrs is required` | URL 里没解析出 mds 地址，检查格式 |
| 启动报 `only one instance allowed per process` | 同进程已经构造过 `RemoteCache`，gflags 是全局的 |
| `Fail to ListMembers` | MDS 不通；查 mds 进程 / 网络 |
| `Fail to put block to remote cache` | cache group 节点不通 / 集群成员不健康，看 dingo-cache 日志 |
| `chunk_hash too short (need > 4 hex chars)` | 上游 key 序列化异常，提 issue |
| Build 报找不到 pybind11 | `pip install pybind11` 或 `apt install pybind11-dev`，并把 cmake prefix 暴露给 dingofs build |
| `Remote connector dingofs missing adapter module_path or class_name` | yaml 里只设了 `remote_storage_plugins: ["dingofs"]`，缺 extra_config 下面的 `module_path` / `class_name` 两条 |
| `No adapter found for URL: plugin://dingofs` | LMCache ≥ 0.4.5，但用的是旧 adapter（只认 `dingofs://`）。升级到本文档对应版本的 adapter；plugin 模式下真实 URL 走 `extra_config.remote_storage_plugin.dingofs.url` |
| `cannot import name '_dingofs_native' from 'dingofs_connector'` | wheel 的 ABI tag 跟 vLLM EngineCore 的 Python 副版本不匹配。用 EngineCore 用的那个 venv 重新 `make` 打 wheel |
| 同时看到 `RemoteBackend-dingofs` 和 `RemoteBackend` 两个 backend 注册 | yaml 同时配了 `remote_storage_plugins` 和 `remote_url` → LMCache 创建两个 backend，会双写。删掉 `remote_url`，URL 改放 extra_config 的 `remote_storage_plugin.dingofs.url` |
| 启动 `ValueError: chunk_size=N yields M.MM MiB per chunk, which exceeds the configured cap of 4 MiB` | LMCache 的 `chunk_size × per_token_kv` 超过了 dingofs cache 节点 `LocalFileSystem` 的 io_uring fixed buffer slot（默认 4 MiB）。按错误提示降 `chunk_size`；若 cache 集群已升级 slot 大小，在 lmcache.yaml 设 `remote_storage_plugin.dingofs.max_chunk_mib: 16` 或 `export DINGOFS_MAX_CHUNK_MIB=16` 放开客户端检查 |
| 启动 `ValueError: dingofs URL no longer accepts query string` | URL 里带了 `?k=v`。dingofs gflag 改放 conf 文件 + `remote_storage_plugin.dingofs.conf` 字段；`max_chunk_mib` 改放 `remote_storage_plugin.dingofs.max_chunk_mib` 字段（或 env）|
| 启动 `unknown command line flag '...'`（来自 gflags 报错） | conf 文件里写了 dingofs 不识别的 gflag 名，typo 检查；该错误来自 `gflags::ReadFromFlagsFile` 的 fatal-mode |

## 性能基线（待补充）

在 N 节点 dingo-cache 集群上，256 KiB chunk × 64 并发：

| 路径 | 吞吐 | p99 延迟 |
|---|---|---|
| Put | TBD | TBD |
| Get | TBD | TBD |
| Batched get (32-key) | TBD | TBD |

底线参照：tmpfs 上的 `fs_l2_adapter`。

## 限制 / 未来工作

- **Get 端到端绝对零拷贝**：当前一次 `IOBuf -> dst` memcpy 是 brpc 物理下限。要消除需要 RDMA 或共享内存协议。
- **Partial chunk 处理**：v1 假设全 chunk 大小。部分读由 LMCache 的 `reshape_partial_chunk` 处理，但 dingofs 当前没暴露 `bytes_read`。
- **GPU 直存**：当前 KV 必须从 GPU 先 offload 到 host。GDR/NVME-of 是后续方向。
