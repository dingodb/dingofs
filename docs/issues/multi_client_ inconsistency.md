# 问题分析：跨客户端写入后 page cache 不一致

## 现象

在 client-1 修改了文件 `test20.dengzh` 之后，client-2 无法看到最新的文件内容。

- **client-1 挂载点**：`/home/dengzihui/mount-test/dengzh_c101_01-1`
- **client-2 挂载点**：`/home/dengzihui/mount-test/dengzh_c101_01-2`

---

## 事件时间线（从日志中提取）

| 时间 | 客户端 | 操作 |
|------|--------|------|
| 08:08:51 | client-2 | **创建** test20.dengzh，写入 **13 字节**，slice=10001203584 写入块存储 → 内核 page cache 中缓存了 13 字节数据 |
| 08:08:57 | client-2 | 读取文件 → 无 `read_slice`，直接从 **page cache** 命中 |
| 08:09:13 | client-1 | 修改文件，写入 **16 字节**，新 slice=10001303840 提交到 MDS，mtime 更新 |
| 08:09:18 | client-2 | `lookup` 返回新 mtime 和 size=16（元数据正确），`open RDONLY → flush → release`，**无任何 FUSE read 调用** → 数据仍从 stale page cache 读取 |

`vfs_meta` 日志证明：client-2 在 client-1 写入之后，始终没有 `read_slice` 操作，说明内核 page cache **从未失效**。

---

## 根本原因

### 问题代码 1：`FUSE_CAP_WRITEBACK_CACHE` 无条件启用

```cpp
// src/client/fuse/fuse_op.cc:89
LOG_IF(INFO, fuse_set_feature_flag(conn, FUSE_CAP_WRITEBACK_CACHE))
    << "[enabled] FUSE_CAP_WRITEBACK_CACHE";  // 始终开启，无条件
```

### 问题代码 2：`keep_cache=1` 默认开启

```cpp
// src/common/options/client.cc:219
DEFINE_bool(fuse_enable_keep_cache, true, "keep file page cache");

// src/client/fuse/fuse_op.cc:583
fi->keep_cache = FLAGS_fuse_enable_keep_cache ? 1 : 0;  // 默认为 1
```

### Linux 内核的关键逻辑

在 `fs/fuse/inode.c::fuse_change_attributes()` 中：

```c
bool is_wb = fc->writeback_cache;  // TRUE，因为 FUSE_CAP_WRITEBACK_CACHE 已启用

if (!is_wb) {  // ← 当 writeback_cache=true 时，整个 if 块被跳过！
    if (oldsize != attr->size) {
        truncate_pagecache(inode, attr->size);
        inval = true;
    } else if (fc->auto_inval_data) {
        // 比较新旧 mtime，mtime 变了就 invalidate
        if (!timespec64_equal(&new_mtime, &old_mtime))
            inval = true;
    }
    if (inval)
        invalidate_inode_pages2(inode->i_mapping);  // ← 永远不会被调用
}
```

**`FUSE_CAP_WRITEBACK_CACHE` 启用时，`AUTO_INVAL_DATA` 基于 mtime 的缓存失效逻辑被完全绕过。**

这两个 capability 对于跨客户端缓存一致性是互斥的：

- `WRITEBACK_CACHE`：内核认为自己是文件写入的权威方，不相信外部（MDS 返回的）mtime 变化，因为内核可能有自己尚未提交的脏页。
- `AUTO_INVAL_DATA`：基于 MDS 返回的 mtime 变化来失效 page cache。

当 `WRITEBACK_CACHE=on` 时，内核绕过 AUTO_INVAL_DATA 检查，即使 client-2 通过 `lookup` 看到了 client-1 写入后的新 mtime，其 page cache 也不会被失效。

`keep_cache=1` 进一步加剧了问题：`open()` 时也不会清除 page cache，旧数据在每次打开文件时都会被保留。

---

## 数据流示意

```
Client-2 page cache: [client-2 写的旧 13 字节]
       │
       ├─ lookup → 从 MDS 获得新 mtime（client-1 写后），size=16  ← 元数据正确
       │   kernel: fuse_change_attributes() 被调用
       │   is_wb = true → if(!is_wb) 块被跳过
       │   invalidate_inode_pages2() 从不调用
       │   page cache 仍是 [旧的 13 字节]
       │
       └─ open + read → 从 stale page cache 返回旧数据   ← 错误！
```

---

## 修复方向

| 方案 | 说明 | 代价 |
|------|------|------|
| 禁用 `FUSE_CAP_WRITEBACK_CACHE` | 最彻底，使 AUTO_INVAL_DATA 生效 | 写入性能下降（每次 write 都同步到 FUSE daemon） |
| 默认 `direct_io=1` | 绕过 page cache，每次读写直接走 FUSE | 读性能下降（无 page cache 加速） |
| 实现 `fuse_lowlevel_notify_inval_inode()` | 当 MDS 发现文件更新时，主动通知其他客户端的 FUSE daemon 失效 inode page cache | 需要 MDS 推送机制（心跳/watch）+ 客户端实现 |
| 降低 `attr_timeout` 为 0 | 每次读都 getattr | 不能解决 WRITEBACK_CACHE 绕过问题，且元数据压力大 |

---

## 相关文件

- `src/client/fuse/fuse_op.cc` — `InitFuseConnInfo()`、`FuseOpOpen()`
- `src/common/options/client.cc` — `fuse_enable_keep_cache`、`fuse_enable_auto_inval_data` 默认值
- `src/client/vfs/vfs_impl.cc` — `GetAttrTimeout()`、`GetEntryTimeout()`（默认 1s）
