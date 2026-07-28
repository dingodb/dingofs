# 在 DingoFS 上运行 xfstests

[English](README.md)

用 [xfstests](https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git/) 测试
DingoFS 的适配层。**xfstests 本体零修改**。

## 工作原理

```
sudo ./check generic/001
   │  读 <xfstests树>/local.config（FSTYP=fuse、FUSE_SUBTYP=.dingofs、
   │  TEST_DIR/SCRATCH_MNT——由 setup.sh 生成）
   ▼
mount -t fuse.dingofs xftest <root>/test
   │  mount(8) 根据类型自动转调 /sbin/mount.fuse.dingofs
   ▼
mount.fuse.dingofs（挂载 helper）
   │  1. 将设备名同时用作 fsname 和挂载 source（xftest）
   │  2. 读 /etc/dingofs-xfstests.conf，解析该 fsname 的 meta-url
   │  3. 拉起：dingo-client <meta-url> <挂载点> --fuse_subtype= ...
   │  4. 轮询到挂载点真正就绪才返回
   ▼
xfstests 在挂载点内执行测试 IO
```

xfstests 采用双盘模型，因此需要**两个独立的 DingoFS 实例**：`xftest`
（TEST 盘——常驻挂载，数据跨用例累积）和 `xfscratch`（SCRATCH 盘——每个
用例前"格式化"并重挂）。xfstests 对 `FSTYP=fuse` 的原生支持使这一切无需
mkfs：`_mkfs_dev` 为空操作，`_scratch_mkfs` 改为挂载后清空文件，
fsck/一致性检查跳过。

## 文件

| 文件 | 用途 |
|---|---|
| `mount.fuse.dingofs` | 挂载 helper（装到 `/sbin`）：meta-url 解析、remount 处理、挂载就绪等待、挂卸周期间等旧实例退出 |
| `setup.sh` | 一键安装器；同时生成 xfstests 树内的 `local.config` 和 `/etc/dingofs-xfstests.conf` |
| `reset.sh` | 卸载两个实例、等待 client 退出，再删除 `<root>/runtime/{xftest,xfscratch}`；仅 local 模式会同时清空文件系统内容 |

## 安装

```bash
# 1. 编译 xfstests（一次性；Debian/Ubuntu 依赖如下）
sudo apt install -y xfslibs-dev libacl1-dev libgdbm-dev
git clone https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git
cd xfstests-dev && make -j8

# 2. 编译 dingo-client（本仓库）

# 3. 安装适配层
bash setup.sh <xfstests树路径> [根目录]
```

`setup.sh` 生成整条链路需要的所有产物：

| 产物 | 位置 | 谁在用 |
|---|---|---|
| 挂载 helper（`mount.fuse.dingofs` 的副本） | `/sbin/mount.fuse.dingofs` | 每次 `mount -t fuse.dingofs` 时 mount(8) 调用 |
| helper 配置（生成） | `/etc/dingofs-xfstests.conf` | helper，每次挂载重读 |
| xfstests 配置（生成） | `<xfstests树>/local.config` | `./check`，每次运行读取 |
| 目录布局 | `<root>/{test,scratch,runtime}` | 挂载点 + 运行数据 |
| `fsgqa` 组及 `fsgqa` / `fsgqa2` / `123456-fsgqa` 用户 | 系统账号 | 权限类用例 |

所有内容收在一个根目录下（默认 `/mnt/dingofs-xfstests`）：

```
<root>/test      TEST_DIR 挂载点
<root>/scratch   SCRATCH_MNT 挂载点
<root>/runtime   每个 fs 的运行数据（leveldb 元数据 / storage / cache / 日志）
```

可选环境变量：

| 变量 | 含义 | 默认值 |
|---|---|---|
| `DINGOFS_CLIENT_BIN` | dingo-client 二进制 | `<仓库>/build/bin/dingo-client` |
| `DINGOFS_META_URL_TEMPLATE` | meta-url 模板，`{fsname}` 会被替换 | 空 = local file 后端，数据在 `<root>/runtime` |
| `DINGOFS_EXTRA_FLAGS` | 附加 client 参数，以空格分隔 | 空 |

重跑 `setup.sh` 会重新生成两个配置文件（手改内容会被覆盖）。重新编译
client 后无需重装——配置里记录的是二进制路径。

## 运行

```bash
cd <xfstests树>
sudo ./check generic/001              # 单个用例
sudo ./check generic/013 generic/070  # 指定列表
sudo ./check -g quick                 # quick 组
sudo ./check -g quick -e generic/062  # 排除已知失败
```

失败后到两处找证据：

- `<xfstests树>/results/generic/NNN.out.bad`（及 `.full`）——测试侧 diff/日志
- `<root>/runtime/<fsname>/run/client.out`——daemonize 之前的启动输出
- `<root>/runtime/<fsname>/log/<pid>.stdout` 及同目录下的 glog 文件——后台
  client 的输出；测试出现 `Transport endpoint is not connected` 时到这里
  查找崩溃栈

在 **local 模式**下，用 `bash reset.sh` 将两个文件系统重置为空状态。脚本
先卸载两个挂载点并等待本套件的 dingo-client 进程退出，然后删除
`<root>/runtime/xftest` 和 `<root>/runtime/xfscratch`（运行中的 client 持有
leveldb/storage 文件，直接删除会损坏状态）。典型时机包括切换后端、失败
用例弄脏 TEST 文件系统或重跑前需要干净基线；日常连续测试无需执行。reset
后直接运行 `./check`，local 文件系统会在首次挂载时重新创建。

在 **MDS 模式**下，`reset.sh` 只卸载 client 并清理本地运行数据，**不会**
删除 MDS 或远端存储中的文件系统内容。需要真正的空文件系统时，应通过对应
集群的管理流程重置。

## 启动后的磁盘布局

两个实例都挂载后，所有内容集中在根目录下：

```
/mnt/dingofs-xfstests/
├── test/        FUSE 挂载点（source=xftest，type=fuse）——虚拟视图，卸载后为空目录
├── scratch/     FUSE 挂载点（source=xfscratch）——同上
└── runtime/
    ├── xftest/                            TEST 盘实例（xfscratch/ 同构，完全隔离）
    │   ├── meta/xftest/                   local 模式 leveldb 元数据：目录树、inode
    │   │   └── *.ldb *.log CURRENT MANIFEST-* LOCK   ← LOCK 就是"等旧实例退出"守的锁
    │   ├── storage/xftest/blocks/...      local file 后端数据块，命名 <blockid>_<offset>_<len>
    │   ├── cache/<uuid>/                  本次挂载实例的读写缓存
    │   ├── log/
    │   │   ├── <pid>.stdout               daemonize 后台进程输出——崩溃栈在这
    │   │   ├── access_<pid>_<date>.log    FUSE access log
    │   │   └── dingo-client.{info,error,fatal}.log.*   glog
    │   └── run/
    │       ├── client.out                 daemonize 之前的启动输出
    │       └── fd_comm_socket.<pid>       热升级 fd 交接 socket
    └── xfscratch/
```

进程视角：每实例一个 dingo-client
（`local://xftest` 端口 11945，`local://xfscratch` 端口 11942）。

MDS 模式下 `meta/` 和 `storage/` 不使用（元数据在 MDS 集群、数据在对象
存储），`runtime/` 只承载 cache、日志和 run 目录。

## 后端选择（meta-url）

helper 每次挂载都按 fsname 解析一次 meta-url，**命中即止**（所有键都在
`/etc/dingofs-xfstests.conf`，每次挂载重读，改完下次挂载即生效）：

1. `META_URL_<fsname>`——按 fs 单独覆盖，如
   `META_URL_xftest='local://xftest?storage=s3&ak=...&endpoint=...'`
2. `META_URL_TEMPLATE`——`{fsname}` 被替换，如
   `META_URL_TEMPLATE='mds://10.232.10.7:7400/{fsname}'`
3. 内置默认——`local://{fsname}?storage=file&path=<root>/runtime/{fsname}/storage`

**local 模式（默认）**：零外部依赖，首次挂载自动建 fs。

**MDS 模式**：MDS/存储集群必须已就绪，且**两个 fs 必须预先建好**（MDS
模式下 client 不会自动建 fs）：

```bash
dingo-cli fs create xftest    ...
dingo-cli fs create xfscratch ...
```

### `/etc/dingofs-xfstests.conf` 键参考

由 `setup.sh` 生成（所有值经 shell 转义），helper 每次挂载时 source——
改完下次挂载生效，无需重装：

| 键 | 含义 |
|---|---|
| `CLIENT_BIN` | helper 拉起的 dingo-client 二进制 |
| `BASE_ROOT` | 每个 fs 的运行数据根（`<root>/runtime`） |
| `META_URL_TEMPLATE` | meta-url 模板，`{fsname}` 被替换（解析第 2 级） |
| `META_URL_<fsname>` | 按 fs 的 meta-url 覆盖（解析第 1 级）；手工添加 |
| `EXTRA_FLAGS` | 附加到 client 命令行的额外 gflags，**空格分隔**（有意按词切分，不支持含空格的值），如 `EXTRA_FLAGS='--v=1 --vfs_access_logging=false'` |
| `PORT_<fsname>` | 按 fs 覆盖 dummy-server 端口（默认：fsname 哈希到 11900–11989 的稳定值）；两实例端口不能相同 |

## 设计要点

- **设备名使用简单的文件系统名**（`xftest` 和 `xfscratch`）：xfstests 会
  将上报的 mount source 与 `$TEST_DEV`/`$SCRATCH_DEV` 原样比较，之后还会
  按设备名卸载 scratch，裸名让 source 比对和按设备卸载都无歧义。helper
  借助现有的 `--fuse_mount_options` 追加 `fsname=<设备名>` 让 client 上报
  设备名作为 mount source：client 先加硬编码的 `fsname=DingoFS:<fs>`、再加
  该 flag 的选项，libfuse 对重复选项后者生效。meta-url 中包含 `&`，不适合
  经过 xfstests 内部未加引号的 shell 展开，因此仍通过配置文件传递。
- **`--fuse_subtype=`（置空）**：helper 以无 subtype 方式挂载，内核上报
  的文件系统类型为纯 `fuse`，与 `FSTYP=fuse` 匹配。若用默认的
  `subtype=dingofs`，类型显示 `fuse.dingofs`，xfstests 的 `_fs_type` 检查
  在启动时就会拒绝。`FUSE_SUBTYP` 只用于选择挂载 helper 的名字，跟内核
  上报的类型无关。（libfuse 官方的 xfstests 适配依赖同一事实：其
  passthrough 示例也是无 subtype 挂载。）
- **挂载就绪等待**：`--daemonize=true` 在 FUSE 挂载完成前就 fork 返回。
  helper 先等待 `findmnt` 上报预期的 source 和 `fuse` 类型，再通过一次
  `statfs` 往返确认后台 client 已能处理请求，而不是留下 `ENOTCONN` 的僵尸
  挂载；mount(8) 一返回 xfstests 就会开始 IO。
- **等旧实例退出**：umount(2) 返回时旧 client 未必完全退出（local 模式
  持有 leveldb LOCK；任何模式都占着 dummy-server 端口），helper 启动新
  实例前先等同名 fs 的旧进程退出。
- **配置文件而非环境变量**：mount(8) 调 helper 时环境可能被剥离，
  `/etc/dingofs-xfstests.conf` 是唯一传参通道；conf 缺失视为安装损坏，
  helper 直接报错退出，不回退到猜测的默认值。
- **确定性 dummy-server 端口**：每个 fsname 经 cksum 哈希到 11900–11989
  的固定端口（`xftest`=11945、`xfscratch`=11942），TEST/SCRATCH 两实例
  并存不冲突，重挂复用同一端口。该端口是 client 的调试 HTTP 接口
  （`curl 127.0.0.1:<端口>/flags/...`）。
- root 挂载自动启用 `allow_other`；`default_permissions` 默认开启
  （权限类用例依赖两者）。
