# Status 使用 SSO 内联存储错误消息

`dingofs::Status`（`src/common/status.h`）原先对所有错误消息做堆分配（`std::unique_ptr<char[]>`），哪怕消息只有几个字节。我们决定改用 `absl::InlinedVector<char, 256>` 做 small-string optimization：不超过 256 字节的消息内联存储、零堆分配；超过 256 字节整体溢出到堆存储，**永不截断**。容量是全库统一的编译期常量 `kMsgCapacity`，调整它只需改一行并重编译。

## Considered Options

- **模板化容量 `Status<N>`（默认 256）**：被否决。全库约 5000 处裸 `Status` 引用都会绑定到默认模板实参，不同容量会产生类型分裂与跨容量转换噪音，而代码库中没有任何真实的多容量需求。
- **固定数组 + 超长截断**：被否决。长错误消息（设备路径、请求 ID、inode 信息等）截断后丢失诊断内容，不可接受。
- **手写两字段/union SSO**：被否决。手写"内联 or 堆"双态在构造、拷贝、移动、析构中的组合逻辑是内存错误高发区；`absl::InlinedVector` 提供经过验证的同等实现（`status.h` 原本就已包含该头文件）。

## Consequences

- `sizeof(Status)` 从约 16 字节增至约 280 字节。Status 在回调链（`StatusCallback = std::function<void(Status)>`）中按值传递，并有少量容器存储（如 `chunk_writer` 的批量结果表），这些路径的内存与拷贝开销上升，换取短消息零堆分配。
- `ToString()` 输出保持逐字节兼容（`src/tools/replay` 的解析器和单元测试依赖 `"Type (errno:N) : msg: msg2"` 格式）。唯一例外：空消息工厂调用（如 `Status::BadFd("")`）不再输出尾部分隔符 `": "`，旧行为属于实现怪癖，解析器将 `: msg` 视为可选段，不受影响。
- MDS 侧的 `Status`（`src/mds/common/status.h`，即 `butil::Status` 别名）是独立类型，不受本次重构影响。
