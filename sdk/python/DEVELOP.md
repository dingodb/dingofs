# DingoFS Python SDK 打包指南

## 概述

`dingofs` 是基于 **nanobind** 的 C++ 扩展包，通过 **scikit-build-core** + **CMake** 构建。
支持两种使用场景：

- **本地开发**：editable 模式，修改 C++ 代码后重新安装即可
- **CI 发布**：通过 cibuildwheel 构建 manylinux wheel，发布到 PyPI

---

## 项目结构

```
dingofs-dev/
├── pyproject.toml               # 构建系统声明 + scikit-build-core + cibuildwheel 配置
├── include/dingofs/             # 公共 C++ SDK 头文件（client.h / meta.h / status.h 等）
├── sdk/
│   ├── CMakeLists.txt           # SDK 总入口，管理 cpp/ 和 python/ 子目录
│   ├── cpp/                     # C++ SDK 实现（dingofs_sdk 静态库）
│   │   ├── binding_client.h/.cc # BindingClient / BindingConfig：语言无关的 shim 层
│   │   ├── client.cc            # 公共 C++ API 实现
│   │   └── CMakeLists.txt
│   └── python/                  # Python SDK
│       ├── CMakeLists.txt       # nanobind 扩展的 CMake 配置
│       ├── librados_version.map # GNU ld >= 2.38 版本符号脚本（修复 librados.a 链接问题）
│       ├── src/
│       │   ├── wrapper.cc           # NB_MODULE 入口，组合各子模块
│       │   ├── status_bindings.cc   # Status 绑定
│       │   ├── meta_bindings.cc     # Attr / DirEntry / FsStat / FileType 等绑定
│       │   └── client_bindings.cc   # BindingClient / BindingConfig 绑定
│       ├── dingofs/             # Python 包（打进 wheel 的纯 Python 层）
│       │   ├── __init__.py      # 公开导出：Client, ScandirIterator, DingoFile, DingofsError, ...
│       │   ├── client.py        # Client + ScandirIterator（路径解析、异常风格）
│       │   ├── file.py          # DingoFile：类文件对象，支持 with/read/write/seek
│       │   ├── config.py        # Config dataclass
│       │   └── exceptions.py    # DingofsError(OSError) 异常类
│       ├── tests/               # 单元测试（mock，无需真实集群）
│       │   ├── __init__.py
│       │   ├── conftest.py      # 共享 fixture：ok_status / err_status / make_attr / make_core
│       │   ├── test_client.py   # Client / ScandirIterator / walk 测试
│       │   └── test_file.py     # DingoFile 测试
│       ├── README.md
│       └── DEVELOP.md
└── examples/
    ├── cpp/                     # C++ 示例
    └── python/                  # Python 示例
        ├── common.py            # 公共配置与辅助函数（make_client）
        ├── statfs.py            # 文件系统用量统计
        ├── mkdir.py             # 创建目录、列举、删除
        ├── write.py             # 创建文件并写入
        ├── read.py              # 写入后读回并校验
        ├── stat.py              # GetAttr / chmod / chown
        ├── rename.py            # 重命名文件
        ├── symlink.py           # 创建软链接并读取
        ├── link.py              # 创建硬链接并验证 nlink
        ├── mknod.py             # MkNod 创建普通文件节点
        ├── xattr.py             # SetXattr / GetXattr / ListXattr / RemoveXattr
        ├── ioctl.py             # FS_IOC_GETFLAGS ioctl
        ├── setflags.py          # SetAttr(kSetAttrFlags) 设置 inode flags
        ├── scandir.py           # scandir 迭代器 + context manager
        ├── walk.py              # walk 递归遍历目录树（topdown=True/False）
        └── gflags_demo.py       # set_option / get_option / list_options / print_options
```

---

## 依赖说明

### 构建工具（pyproject.toml 声明，pip 自动安装）

| 依赖 | 版本 | 用途 |
|------|------|------|
| scikit-build-core | >=0.9 | 构建后端，替代 setup.py，纯声明式调用 CMake |

### C++ 依赖（需预先安装）

| 变量 | 默认路径 | 说明 |
|------|----------|------|
| `THIRD_PARTY_INSTALL_PATH` | `$HOME/.local/dingo-eureka` | nanobind、glog、brpc 等三方库 |
| `DINGOSDK_INSTALL_PATH` | `$HOME/.local/dingo-sdk` | dingo-sdk（KV 层） |

> 两个路径均由根目录 `CMakeLists.txt` 从环境变量自动读取，无需额外传 cmake 参数。
> nanobind 通过 `THIRD_PARTY_INSTALL_PATH/nanobind/cmake` 的 CMake config 文件定位，
> 无需安装为 Python 包。

---

## 构建流程

scikit-build-core 的完整构建链：

```
pip install --no-build-isolation -e .
        │
        ▼
   pyproject.toml         读取 build-system.requires
        │                  在当前环境安装 scikit-build-core
        ▼
scikit_build_core.build   读取 [tool.scikit-build]
        │                  cmake.args:
        │                    -DBUILD_PYTHON_BINDINGS=ON
        │                    -DBUILD_UNIT_TESTS=OFF
        │                    -DBUILD_INTEGRATION_TESTS=OFF
        │                    -DDINGOFS_BUILD_SDK=OFF
        ▼
  cmake <root>             编译整个 dingofs 项目（含 dingofs_sdk 静态库）
        ▼
  cmake --build            链接 _dingofs_core.cpython-*.so
        ▼
  cmake --install          install(TARGETS _dingofs_core DESTINATION dingofs)
        │                  产物落在 wheel 的 dingofs/ 子目录
        ▼
     wheel                 dingofs/ Python 包 + _dingofs_core.so → .whl 文件
```

---

## 单元测试

测试使用 mock，**无需真实 DingoFS 集群**。

### 安装测试依赖

```bash
pip install pytest
```

### 运行所有测试

```bash
# 在项目根目录执行
cd /path/to/dingofs-dev
python3 -m pytest sdk/python/tests/ -v
```

### 常用选项

```bash
# 只跑某个测试类
python3 -m pytest sdk/python/tests/test_client.py::TestScandir -v

# 只跑某个测试方法
python3 -m pytest sdk/python/tests/test_client.py::TestWalk::test_walk_topdown -v

# 静默模式（只显示失败）
python3 -m pytest sdk/python/tests/ -q
```

### 测试结构

| 文件 | 覆盖范围 |
|------|----------|
| `tests/conftest.py` | 共享 fixture：`ok_status` / `err_status` / `make_attr` / `make_core` / `make_dir_entry` |
| `tests/test_client.py` | `Client` 全部方法、`ScandirIterator`、`walk()` |
| `tests/test_file.py` | `DingoFile` 读写 / seek / flush / fsync / close / context manager |

---

## 本地开发

### 前置条件

1. 已安装 cmake >= 3.17、gcc / clang（支持 C++17）
2. 已安装 dingo-eureka 依赖到 `$HOME/.local/dingo-eureka`（含 nanobind）
3. Python >= 3.9

### Editable 安装（推荐开发时使用）

```bash
# 在项目根目录执行
cd /path/to/dingofs-dev

pip install --no-build-isolation -e .
```

> **必须加 `--no-build-isolation`**
> 否则 pip 会创建隔离的临时虚拟环境，找不到 C++ 依赖。

### 自定义依赖路径

```bash
THIRD_PARTY_INSTALL_PATH=/custom/eureka \
DINGOSDK_INSTALL_PATH=/custom/sdk \
pip install --no-build-isolation -e .
```

### 并行加速编译

```bash
CMAKE_BUILD_PARALLEL_LEVEL=16 pip install --no-build-isolation -e .
```

### 验证安装

```bash
python3 -c "
from dingofs import _dingofs_core as core
print([x for x in dir(core) if not x.startswith('_')])
# 预期输出：['Attr', 'BindingClient', 'BindingConfig', 'DirEntry', ...]
"
```

### 修改 C++ 代码后重新编译

editable 模式下，纯 Python 文件（`sdk/python/dingofs/*.py`）无需重新安装，改了即生效。
修改 C++ 代码后需重新执行：

```bash
pip install --no-build-isolation -e .
```

---

## 构建发布用 wheel

```bash
cd /path/to/dingofs-dev

# 安装构建前端
pip install build

# 构建 wheel（非 editable）
python -m build --no-isolation --wheel .

# wheel 文件在 dist/ 目录
ls dist/
# dingofs-0.1.0-cp310-cp310-linux_x86_64.whl
```

---

## CI 打包（cibuildwheel）

### 触发条件

- push 到 `main` 分支 → 构建 wheel + 发布到 PyPI
- PR 到 `main` 分支 → 仅构建 wheel（不发布）

### cibuildwheel 配置（`pyproject.toml`）

```toml
[tool.cibuildwheel]
before-build = "rm -rf {project}/build"    # 清理上次构建残留
build = "*-manylinux*"                     # 只构建 manylinux 格式
skip = "*-musllinux*"                      # 跳过 musl libc
manylinux-x86_64-image = "dingodatabase/dingo-eureka:manylinux_2_34-fs"

[tool.cibuildwheel.linux]
archs = ["x86_64"]
```

### 关键：自定义 manylinux 镜像

`dingodatabase/dingo-eureka:manylinux_2_34-fs` 是基于官方 manylinux_2_34 定制的镜像，
预装了所有 C++ 依赖（nanobind、glog、brpc、librados 等），对应本地的 dingo-eureka 安装包。
CI 内部无需额外设置 `THIRD_PARTY_INSTALL_PATH`，镜像默认路径与 CMake 约定一致。

### 本地运行 cibuildwheel（调试用）

```bash
pip install cibuildwheel
cd /path/to/dingofs-dev
cibuildwheel --platform linux
# wheel 产物在 wheelhouse/ 目录
```

---

## 环境变量参考

| 变量 | 场景 | 说明 |
|------|------|------|
| `THIRD_PARTY_INSTALL_PATH` | 本地 / CI | C++ 三方依赖路径，默认 `$HOME/.local/dingo-eureka` |
| `DINGOSDK_INSTALL_PATH` | 本地 / CI | dingo-sdk 路径，默认 `$HOME/.local/dingo-sdk` |
| `CMAKE_BUILD_PARALLEL_LEVEL` | 本地 | 并行编译线程数，建议设为 CPU 核心数 |
| `CMAKE_ARGS` | 本地 / CI | 追加额外的 cmake 参数，空格分隔 |

---

## 常见问题

**Q: `find_package(nanobind) failed`**

检查 `THIRD_PARTY_INSTALL_PATH` 是否正确，且目录下存在：
```
$THIRD_PARTY_INSTALL_PATH/nanobind/cmake/nanobind-config.cmake
```

**Q: import 时报 `undefined symbol`**

说明 C++ 链接依赖缺失。请确认使用的 dingo-eureka 版本与本地编译环境一致。
排查方法：
```bash
python3 -c "import dingofs._dingofs_core" 2>&1
ldd $(python3 -c "import dingofs._dingofs_core as m; print(m.__file__)")
```

**Q: 编译后 import 版本不匹配**

确认编译与运行使用同一个 Python：
```bash
python3 --version
ls sdk/python/dingofs/_dingofs_core.cpython-*.so   # 文件名中的版本号应匹配
```

**Q: editable 模式下 C++ 修改未生效**

editable 模式只做了一次 cmake build，C++ 修改后需重新执行：
```bash
pip install --no-build-isolation -e .
```
