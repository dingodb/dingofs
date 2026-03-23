# dingofs

Python SDK for [DingoFS](https://github.com/dingodb/dingofs) — a high-performance distributed filesystem.

Provides a pure-Python API to read, write, and manage files on a DingoFS cluster **without FUSE mounting**.

## Requirements

- Linux x86_64
- Python >= 3.9
- A running DingoFS cluster (MDS reachable)

## Installation

```bash
pip install dingofs
```

## Quick Start

```python
import dingofs

# Build and start the client
client = dingofs.Client.build(
    mds_addrs="10.232.10.5:8801",
    fs_name="myfs",
    mount_point="/dingofs/client/mnt/myfs",
)

# Create a directory
client.mkdir("/data", 0o755)

# Write a file
with client.open("/data/hello.txt", "wb") as f:
    f.write(b"hello dingofs")

# Read a file
with client.open("/data/hello.txt", "rb") as f:
    print(f.read())   # b'hello dingofs'

# List a directory
for entry in client.listdir("/data"):
    print(entry.name, entry.ino)

client.stop()
```

All methods raise `dingofs.DingofsError` (a subclass of `OSError`) on failure.

## API Reference

### `Client.build()`

```python
client = dingofs.Client.build(
    mds_addrs="host:port,...",   # MDS 地址列表，逗号分隔
    fs_name="myfs",              # 文件系统名称
    mount_point="/path/...",     # 在 MDS 注册的挂载点路径
    conf_file="",                # 可选：client.conf 文件路径
    log_dir="",                  # 可选：日志目录
    log_level="",                # 可选：日志级别
)
```

### 文件系统操作

| 方法 | 返回 | 说明 |
|------|------|------|
| `statfs()` | `FsStat` | 获取文件系统用量统计 |
| `stat(path)` | `Attr` | 获取文件/目录属性 |
| `set_attr(path, mask, attr)` | `Attr` | 设置文件属性 |
| `chmod(path, mode)` | `Attr` | 修改权限位 |
| `chown(path, uid, gid)` | `Attr` | 修改属主（`-1` 表示不修改） |

### 目录操作

| 方法 | 返回 | 说明 |
|------|------|------|
| `mkdir(path, mode)` | `Attr` | 创建目录 |
| `rmdir(path)` | `None` | 删除空目录 |
| `listdir(path)` | `list[DirEntry]` | 一次性返回目录所有条目 |
| `scandir(path)` | `ScandirIterator` | 懒加载迭代器，按批拉取条目 |
| `walk(top, topdown=True)` | `Generator` | 递归遍历目录树，yield `(dirpath, dirnames, filenames)` |

#### `scandir()` 示例

```python
# 基本迭代
for entry in client.scandir("/data"):
    print(entry.name, entry.ino)

# 配合 with 语句：提前 break 时句柄立即释放
with client.scandir("/data") as it:
    for entry in it:
        if entry.name == "target":
            break
```

`ScandirIterator` 每次从服务端拉取 64 条条目，支持提前退出（`break` / `close()`），
目录句柄在迭代结束或 `close()` 时释放。

#### `walk()` 示例

```python
# topdown=True（默认）：父目录先于子目录
for dirpath, dirnames, filenames in client.walk("/data"):
    for fname in filenames:
        print(f"{dirpath}/{fname}")

# topdown=False：子目录先于父目录（适合递归删除）
for dirpath, dirnames, filenames in client.walk("/data", topdown=False):
    ...
```

### 文件操作

```python
with client.open(path, mode) as f:
    f.write(b"data")
    f.read(1024)
    f.seek(0)
    f.tell()
    f.flush()
    f.fsync()
```

`open()` 返回 `DingoFile`，支持 Python 标准 `mode` 字符串：

| mode | 说明 |
|------|------|
| `"r"` / `"rb"` | 只读 |
| `"w"` / `"wb"` | 只写，创建或截断 |
| `"a"` / `"ab"` | 追加写 |
| `"r+"` / `"r+b"` | 读写（文件必须存在） |
| `"w+"` / `"w+b"` | 读写，创建或截断 |
| `"a+"` / `"a+b"` | 读写，追加 |

其他文件操作：

| 方法 | 返回 | 说明 |
|------|------|------|
| `mknod(path, mode, dev)` | `Attr` | 创建文件节点 |
| `unlink(path)` | `None` | 删除文件 |
| `rename(src, dst)` | `None` | 重命名/移动 |

### 链接

| 方法 | 返回 | 说明 |
|------|------|------|
| `link(src, dst)` | `Attr` | 创建硬链接 |
| `symlink(target, link)` | `Attr` | 创建符号链接 |
| `readlink(path)` | `str` | 读取符号链接目标 |

### 扩展属性

| 方法 | 返回 | 说明 |
|------|------|------|
| `setxattr(path, name, value, flags)` | `None` | 设置扩展属性 |
| `getxattr(path, name)` | `str` | 读取扩展属性 |
| `listxattr(path)` | `list[str]` | 列举扩展属性名 |
| `removexattr(path, name)` | `None` | 删除扩展属性 |

### Ioctl

```python
out = client.ioctl(path, cmd, in_buf=b"", out_size=8, flags=0)
```

### 运行时选项

```python
Client.set_option("log_level", "WARNING")
Client.get_option("vfs_meta_rpc_timeout_ms")   # "10000"
Client.list_options()                           # list[OptionInfo]
Client.print_options()                          # 打印到 stdout
```

### `SET_ATTR_*` 掩码常量

用于 `set_attr()` 的 `mask` 参数：

```python
dingofs.SET_ATTR_MODE        # 权限位
dingofs.SET_ATTR_UID         # 属主 UID
dingofs.SET_ATTR_GID         # 属主 GID
dingofs.SET_ATTR_SIZE        # 文件大小（truncate）
dingofs.SET_ATTR_ATIME       # 访问时间
dingofs.SET_ATTR_MTIME       # 修改时间
dingofs.SET_ATTR_ATIME_NOW   # 访问时间设为当前
dingofs.SET_ATTR_MTIME_NOW   # 修改时间设为当前
dingofs.SET_ATTR_CTIME       # 元数据修改时间
dingofs.SET_ATTR_FLAGS       # inode flags
```

### 异常

所有方法在失败时抛出 `dingofs.DingofsError`：

```python
from dingofs import DingofsError
import errno

try:
    client.stat("/nonexistent")
except DingofsError as e:
    print(e.dingofs_errno)   # errno 值，如 errno.ENOENT
    print(str(e))            # 错误描述
```

## Examples

更多示例见 [examples/python/](../../examples/python/) 目录：

| 文件 | 说明 |
|------|------|
| `statfs.py` | 文件系统用量统计 |
| `mkdir.py` | 创建目录、列举、删除 |
| `write.py` | 创建文件并写入 |
| `read.py` | 写入后读回并校验 |
| `stat.py` | GetAttr / chmod / chown |
| `rename.py` | 重命名文件 |
| `symlink.py` | 软链接 |
| `link.py` | 硬链接 |
| `mknod.py` | MkNod |
| `xattr.py` | 扩展属性 |
| `ioctl.py` | FS_IOC_GETFLAGS |
| `setflags.py` | inode flags |
| `scandir.py` | scandir 迭代器 + context manager |
| `walk.py` | walk 递归遍历目录树 |
| `gflags_demo.py` | 运行时选项 API |

运行任意示例（先编辑 `examples/python/common.py` 填入集群地址）：

```bash
cd examples/python
python3 scandir.py
```

## License

Apache License 2.0
