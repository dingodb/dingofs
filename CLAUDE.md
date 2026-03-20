# CLAUDE.md

本文件为 Claude Code (claude.ai/code) 在本仓库中工作时提供指导。

## 项目概述

DingoFS 是一个云原生分布式文件系统，支持 POSIX 兼容、多级缓存（内存、本地 SSD、分布式集群）和 S3 兼容。使用 C++17 编写，基于 bRPC 框架。

## 构建命令

```bash
# 初始化子模块（首次需要）
git submodule sync && git submodule update --init --recursive

# 完整构建 启用单元测试
mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_UNIT_TESTS=ON .. && make -j 12

# 完整构建 关闭单元测试
mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_UNIT_TESTS=OFF .. && make -j 12

```

### 关键 CMake 选项

| 选项 | 默认值 | 说明 |
|------|--------|------|
| `BUILD_UNIT_TESTS` | OFF | 启用单元测试 |
| `BUILD_INTEGRATION_TESTS` | OFF | 启用集成测试 |
| `USE_TCMALLOC` | ON | 使用 tcmalloc 内存分配器 |
| `CMAKE_BUILD_TYPE` | Debug | Debug/Release/RelWithDebInfo |

### 依赖项

第三方库安装到 `~/.local/dingo-eureka`，Dingo SDK 安装到 `~/.local/dingo-sdk`。可通过环境变量 `THIRD_PARTY_INSTALL_PATH` 和 `DINGOSDK_INSTALL_PATH` 覆盖。

## 运行测试

```bash
# 运行所有单元测试（需先使用 -DBUILD_UNIT_TESTS=ON 构建）
./build/bin/test/test_mds

# 测试使用 Google Test 框架
# 测试二进制文件输出到：build/bin/test/
```

## 代码风格

- `.clang-format`：Google 风格，80 列限制
- `.clang-tidy`：强制命名规范：
  - 类/方法：`CamelCase`（驼峰命名）
  - 成员变量/变量/参数：`lower_case`（小写下划线）
  - 私有成员：`lower_case_`（尾部下划线）
  - 静态常量：`kCamelCase`
  - 宏：`UPPER_CASE`（全大写）
  - 命名空间：`lower_case`（小写下划线）

## 代码设计
- 遵循经典软件工程的原则，比如：KILL原则、DRY原则、SOLID原则等
- 代码实现要求简洁优雅
- 不要过度设计，过度抽象
- 保证代码可读性，同时也要高性能

## 架构

### 主要组件

| 组件 | 位置 | 二进制文件 | 说明 |
|------|------|-----------|------|
| MDS | `src/mds/` | `dingo-mds` | 元数据服务 |
| Client | `src/client/` | `dingo-client` | 基于 FUSE 的文件系统客户端 |
| Cache | `src/cache/` | `dingo-cache` | 多级缓存层 |
| Common | `src/common/` | — | 共享工具库 |

### MDS 内部结构

- **`src/mds/storage/storage.h`** — 抽象 `KVStorage` 和 `Txn` 接口（Put/Get/Scan/Delete/NewTxn）
- **`src/mds/filesystem/filesystem.h`** — 核心 `FileSystem` 类，实现所有 POSIX 操作
- **`src/mds/service/`** — bRPC 服务处理器
- **`src/mds/server.cc`** — 服务入口，选择存储引擎

### 存储后端

- **DingoStore** (`dingodb_storage.*`) — 生产环境分布式后端
- **TiKV** (`tikv_storage.*`) — 基于 TiKV 的后端
- **Dummy** (`dummy_storage.*`) — 内存后端，用于测试

### RPC 与序列化

- **框架**：bRPC，使用 bthread 用户态线程
- **Protobuf 定义**：`proto/dingofs/`（来自 `dingofs-proto` 的 git 子模块）
- **主服务**：`proto/dingofs/mds.proto` — `MDSService`，包含 50+ RPC 方法
- **错误码**：`proto/dingofs/error.proto`
- **生成代码**：`build/protos/dingofs/*.pb.{h,cc}`

### 关键类型与模式

- **Status**：`butil::Status`（来自 bRPC）— 所有操作的返回值类型，定义在 `src/mds/common/status.h`
- **异步 I/O**：bthread + `BthreadSemaphore`，位于 `src/mds/common/synchronization.h`
- **事务**：支持 `ReadCommitted` 和 `SnapshotIsolation` 隔离级别
- **作用域守卫**：`ON_SCOPE_EXIT` 宏，位于 `synchronization.h`

### 测试结构

- `test/unit/mds/` — MDS 单元测试（filesystem、storage、service、common）
- 测试使用 `DummyStorage` 后端进行隔离
- 命名空间：`dingofs::mds::unit_test`

## 配置

模板：`scripts/dev-mds/mds.template.conf`
- 存储引擎：`dingo-store`、`tikv` 或 `dummy`
- 存储 URL 格式：`file://./conf/coor_list`、`list://ip:port,...` 或直接 `ip:port,...`
