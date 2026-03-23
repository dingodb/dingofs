# TiKV Go Client 存储后端设计文档

## 概述

本文档描述 MDS 服务新增 `tikv-go` 存储后端的设计与实现方案。

新后端使用 Go TiKV Client SDK（`github.com/tikv/client-go/v2`），通过 CGO 编译为 C 静态库供 C++ 调用，与现有 `tikv`（Rust CXX bridge）后端并存，由启动参数 `--mds_storage_engine=tikv-go` 选择。

---

## 背景与动机

MDS 服务通过 `KVStorage` 抽象接口支持多种存储后端（`dingo-store`、`tikv`、`dummy`）。现有 TiKV 后端通过 Rust CXX bridge 实现，维护成本较高。Go Client SDK 生态更活跃、接口更简洁，因此新增 `tikv-go` 后端作为替代选择。

---

## 关键约束

MDS 使用 brpc 的 bthread 执行模型。bthread 是用户态协程，被调度在系统线程（pthread）上运行。**CGO 调用会将当前 goroutine 固定到某个 OS 线程上，若 CGO 函数阻塞，则该 pthread 被阻塞，导致其上的所有 bthread 无法调度。**

因此，所有涉及网络 I/O 的 TiKV 操作（Get/BatchGet/Scan/Commit/Rollback）**必须异步执行**，不得在 CGO 调用栈上阻塞。

---

## 架构设计

采用**异步回调模式**：

```
bthread (C++)                         goroutine (Go)
     |                                     |
     | 1. 调用 async CGO 函数               |
     |    (传入 callback + ctx)             |
     |  ────────────────────────────>       |
     |                                2. 启动 goroutine
     | 3. BthreadSemaphore::Acquire()      执行 TiKV 操作
     |    (butex_wait，让出 bthread,        |
     |     不阻塞 pthread)             3. 操作完成
     |    ...                              |
     |  <────────────────────────────       |
     |    4. goroutine 调用 C callback      |
     |       (写入结果 + sem.Release(1))    |
     | 5. bthread 被唤醒，读取结果          |
```

**关键点**：
- Go goroutine 在独立的 OS 线程上执行，不占用 bthread 的 pthread
- `BthreadSemaphore::Acquire()` 内部使用 `butex_wait`，会让出当前 bthread（但不阻塞底层 pthread）
- `BthreadSemaphore::Release(1)` 内部使用 `butex_wake`，可从任意线程（包括 Go goroutine 所在线程）安全调用

---

## 文件结构

```
src/mds/storage/
├── tikv_go/
│   ├── bridge.go          # CGO 异步桥接核心
│   ├── types.go           # C 类型定义 + 回调辅助函数
│   ├── go.mod             # Go 模块配置
│   └── CMakeLists.txt     # 构建 libtikvgo.a
├── tikv_go_storage.h      # C++ wrapper 类声明 + AsyncContext
└── tikv_go_storage.cc     # C++ 异步 wrapper 实现
```

---

## Go CGO Bridge

### C 类型定义（types.go）

```c
// 通用异步结果（Get/Put/Delete/Commit/Rollback）
typedef struct {
    char* data;       // 结果数据（NULL 表示无数据或 key 不存在）
    int   data_len;
    char* error;      // 错误信息（NULL 表示成功）
    int   error_len;
} CAsyncResult;

// KV 对
typedef struct {
    char* key;   int key_len;
    char* value; int value_len;
} CKVPair;

// KV 数组结果（BatchGet/Scan）
typedef struct {
    CKVPair* pairs;
    int      count;
    char*    error;
    int      error_len;
} CAsyncKVResult;

// 回调函数类型（callback/ctx 以 size_t 传递，与 Rust async API 一致）
typedef void (*tikv_go_callback)(size_t ctx, CAsyncResult* result);
typedef void (*tikv_go_kv_callback)(size_t ctx, CAsyncKVResult* result);
```

Go 不能直接调用 C 函数指针，通过 inline helper 间接调用：

```c
static inline void call_callback(size_t cb, size_t ctx, CAsyncResult* result) {
    ((tikv_go_callback)cb)(ctx, result);
}
```

### 导出函数列表（bridge.go）

| 函数 | 类型 | 说明 |
|------|------|------|
| `tikv_go_client_new(addrs, count, out_error, out_error_len)` | 同步 | 创建 txnkv.Client（初始化时调用一次） |
| `tikv_go_client_destroy(handle)` | 同步 | 销毁 client |
| `tikv_go_txn_begin(client, isolation, out_error, out_error_len)` | 同步 | 开启事务（轻量，TSO 获取） |
| `tikv_go_txn_id(txn)` | 同步 | 返回事务 StartTS |
| `tikv_go_txn_destroy(txn)` | 同步 | 释放 cgo.Handle |
| `tikv_go_txn_get_async(txn, key, klen, cb, ctx)` | **异步** | 异步 Get |
| `tikv_go_txn_put_async(txn, key, klen, val, vlen, cb, ctx)` | **异步** | 异步 Put（本地 buffer 写入） |
| `tikv_go_txn_delete_async(txn, key, klen, cb, ctx)` | **异步** | 异步 Delete（本地 buffer 标记） |
| `tikv_go_txn_batch_get_async(txn, keys, lens, count, cb, ctx)` | **异步** | 异步 BatchGet |
| `tikv_go_txn_scan_async(txn, start, slen, end, elen, limit, cb, ctx)` | **异步** | 异步 Scan（包含两端） |
| `tikv_go_txn_commit_async(txn, cb, ctx)` | **异步** | 异步 Commit |
| `tikv_go_txn_rollback_async(txn, cb, ctx)` | **异步** | 异步 Rollback |
| `tikv_go_free_async_result(r)` | 同步 | 释放 CAsyncResult |
| `tikv_go_free_kv_result(r)` | 同步 | 释放 CAsyncKVResult |
| `tikv_go_free_string(s)` | 同步 | 释放错误字符串 |

### 内存管理原则

- 结果内存由 Go 通过 `C.malloc` / `C.CBytes` / `C.CString` 在 C 堆上分配
- C++ 侧通过 `tikv_go_free_*` 释放，不得使用 `free()` 直接释放
- CGO 规则：Go goroutine 在 Go 堆上复制所有 C 指针数据，启动 goroutine 前不持有 C 指针

### Scan 边界处理

Go `Iter(start, upperBound)` 的 `upperBound` 是 **exclusive**。为实现两端包含的语义：
1. 计算 `upperBound = end + 1`（`incrementBytes` 函数处理字节溢出边界情况）
2. 迭代中额外检查 `key <= end`（双重保障）

---

## C++ Wrapper

### AsyncContext

```cpp
struct AsyncContext {
    BthreadSemaphore sem{0};      // butex-based 信号量
    CAsyncResult*    result{nullptr};
    CAsyncKVResult*  kv_result{nullptr};
};
```

生命周期：栈上分配，bthread 在 `sem.Acquire()` 处阻塞等待，Go goroutine 完成后写入结果并调用 `sem.Release(1)`，bthread 被唤醒后读取结果。Go goroutine 调用回调时，AsyncContext 一定存在（bthread 还在等待）。

### 每个操作的执行流程（以 Get 为例）

```cpp
Status TikvGoTxn::Get(const std::string& key, std::string& value) {
    AsyncContext ctx;                                        // 1. 栈上分配上下文
    tikv_go_txn_get_async(txn_handle_,
        key.data(), key.size(),
        (GoUintptr)OnAsyncComplete,                         // 2. 传入回调函数指针
        (GoUintptr)&ctx);                                   //    和上下文指针
    ctx.sem.Acquire();                                      // 3. butex_wait（让出 bthread）
    Status status = ParseAsyncResult(ctx.result, &value);   // 4. 读取结果
    tikv_go_free_async_result(ctx.result);                  // 5. 释放内存
    return status;
}

// Go goroutine 完成后调用（在任意 pthread 上）：
static void TikvGoTxn::OnAsyncComplete(size_t ctx_ptr, CAsyncResult* result) {
    auto* ctx = reinterpret_cast<AsyncContext*>(ctx_ptr);
    ctx->result = result;
    ctx->sem.Release(1);  // butex_wake，唤醒 bthread
}
```

### Commit 错误处理

```cpp
Status TikvGoTxn::Commit() {
    // ... 异步 commit ...
    if (failed) {
        // 异步回滚（commit 失败时自动 rollback）
        AsyncContext rb_ctx;
        tikv_go_txn_rollback_async(..., &rb_ctx);
        rb_ctx.sem.Acquire();
        tikv_go_free_async_result(rb_ctx.result);
    }
    committed_.store(true);
    return status;
}
```

析构时若未 commit 会自动调用 `Commit()`，与现有 TikvTxn 行为一致。

### 隔离级别

| C++ `Txn::IsolationLevel` | 传递给 Go | Go SDK 行为 |
|--------------------------|-----------|------------|
| `kReadCommitted = 0` | `isolation = 0` | 开启 RC 模式 snapshot |
| `kSnapshotIsolation = 1` | `isolation = 1` | 默认 SI 模式 |

---

## CMake 构建集成

### tikv_go/CMakeLists.txt

```cmake
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libtikvgo.a
         ${CMAKE_CURRENT_BINARY_DIR}/libtikvgo.h
  COMMAND CGO_ENABLED=1 go build -buildmode=c-archive
          -o ${CMAKE_CURRENT_BINARY_DIR}/libtikvgo.a .
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building TiKV Go CGO static library"
)
add_custom_target(tikvgo_build DEPENDS ...)
add_library(tikvgo STATIC IMPORTED GLOBAL)
set_target_properties(tikvgo PROPERTIES IMPORTED_LOCATION ${TIKVGO_LIB})
set(TIKVGO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)
```

### storage/CMakeLists.txt 变更

```cmake
add_subdirectory(tikv_go)
target_include_directories(mds_storage PRIVATE ${TIKVGO_INCLUDE_DIR})
target_link_libraries(mds_storage tikvgo mds_common
                      PROTO_OBJS protobuf::libprotobuf pthread resolv)
```

`pthread` 和 `resolv` 是 Go runtime 静态库的必要链接依赖。

---

## 注册新引擎

### server.cc

```cpp
#include "mds/storage/tikv_go_storage.h"
// ...
} else if (FLAGS_mds_storage_engine == "tikv-go") {
    kv_storage_ = TikvGoStorage::New();
}
```

### filesystem.cc

```cpp
DEFINE_string(mds_storage_engine, "dummy",
    "mds storage engine, e.g dingo-store|tikv|tikv-go|dummy");
DEFINE_validator(mds_storage_engine, [](const char*, const std::string& value) -> bool {
    return value == "dingo-store" || value == "tikv" || value == "tikv-go" || value == "dummy";
});
```

---

## 风险与对策

| 风险 | 对策 |
|------|------|
| CGO 回调线程安全 | `BthreadSemaphore::Release` 使用 atomic + `butex_wake`，可从任意线程安全调用 |
| AsyncContext 生命周期 | ctx 在 bthread 栈上，bthread 在 `Acquire()` 等待直到回调完成，生命周期安全 |
| Scan 边界语义差异 | Go Iter upperBound exclusive → `incrementBytes(end)` + 循环内二次检查 |
| CGO 内存管理 | 结果由 Go 通过 C.malloc 分配，C++ 通过 `tikv_go_free_*` 释放 |
| Go runtime 信号冲突 | Go 1.17+ 与 C 信号处理协作良好，无需特殊处理 |
| key/value 含 null 字节 | 全部使用 `(ptr, len)` 传递，不依赖 C 字符串 |
| Put/Delete 是内存操作 | Go SDK `Set()`/`Delete()` 写入本地 buffer，不涉及网络 I/O；使用 async 接口是为保持 API 一致性 |

---

## 构建要求

- **Go 1.25.6+**（本地 `client-go` 依赖要求，`go.mod` 的 `go` 指令设为 `go 1.25.6`）
- 首次构建前需在 `src/mds/storage/tikv_go/` 目录运行：
  ```bash
  cd src/mds/storage/tikv_go
  go mod tidy   # 需要网络访问以填充 go.sum
  ```
- CMake 构建时自动调用 `go build -buildmode=c-archive` 生成 `libtikvgo.a` 和 `libtikvgo.h`

---

## 验证方法

1. **编译验证**：`cmake --build` 成功，无链接错误
2. **单元测试**：参考 `test/unit/mds/storage/test_tikv_storage.cc` 编写 `test_tikv_go_storage.cc`
3. **集成测试**：本地 TiKV 集群 + `--mds_storage_engine=tikv-go`
4. **异步验证**：bthread worker 线程数 < 并发请求数时，确认 worker 不被阻塞（吞吐量正常）
