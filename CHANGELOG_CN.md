# DINGOFS 变更日志
此项目所有重要的变更皆记录于此文件中

## [5.1.0]

**客户端 (Client)**
- 新增 inode 缓存，提升元数据读性能
- 优化元数据系统内部数据结构，提升并发性能
- 新增 standalone 模式，内置元数据存储
- 新增 dashboard 展示页面
- 支持客户端进行 chunk compact
- 支持异步 open/close
- 优化 slice 管理策略，提升 readslice/writeslice 性能
- 优化 file session 管理策略
- 优化 readdir 实现逻辑
- 支持在客户端面板中展示 FUSE 连接信息
- 支持在监控页面展示 chunk 缓存统计信息
- 新增块缓存统计到监控面板
- 新增 `client_access_logging_verbose` 标志，支持详细访问日志
- 支持预取一个或多个数据块
- 支持 FUSE 读零拷贝
- 预取任务改为异步提交
- 重构 VFS 块预取逻辑
- 重构 warmup，进度统计改为按块计算（原为按文件）
- 优化写路径、随机读及预读性能
- 重构 umount 客户端逻辑
- 移除 unix socket path gflag，将 inode_blocks_service gflags 改为常量
- 修复 FUSE attr/entry 缓存超时配置问题
- 修复跨多个文件描述符的 read-after-write 问题
- 修复 compact chunk 及 compact 超时问题
- 修复 chunk 版本断言失败问题
- 修复文件关闭与 invalidate 的死锁问题
- 修复 ctime/mtime 异常问题
- 修复预取块上限被限制为 4G 的问题；修复预取遗漏部分块的问题
- 修复读异常导致文件被截断的问题
- 修复更新 chunk 缓存导致读数据损坏的问题
- 修复构建元数据失败时的 coredump 问题
- 修复挂载已删除文件系统时 `dingo-fuse` 挂起的问题
- 修复使用 `fuse_reply_iov` 且 iov 过大时读文件挂起的问题
- 修复并发写/flush 场景下 flush 序列错误的问题
- 修复读取数据不正确的问题
- 修复读路径中的 coredump 问题

**缓存 (BlockCache)**
- 提升缓存整体读写性能
- 所有文件读写采用 direct I/O，并优化 io_uring 使用方式
- 优化链接并实现固定连接池
- 优化 IOBuffer 拷贝操作，支持移动语义
- 修复 `--cache_dir` 参数不支持分号分隔缓存目录的问题
- 修复 `--conf` 参数在某些情况下不生效的问题
- 修复缓存比例设置不生效的问题
- 修复缓存节点无法正常退出的问题
- 修复启动时终端输出错误缓存大小的问题
- 修复预取失败时数据未清理的问题
- 新增存储上传重试超时机制
- 缓存节点启动失败时记录错误日志

**元数据服务 (MDS)**
- 支持 TiKV 作为后端存储
- 优化 store 操作调度线程
- 支持客户端 RPC 故障转移
- 支持在 FUSE 层缓存文件和目录读请求
- 支持通过 `statfs` 获取目录配额信息
- 支持将已删除的文件系统重命名，避免与同名新文件系统冲突
- 支持校验队列等待超时
- 支持展示 MDS 缓存摘要信息
- 新增 dingo-sdk 事务的 trace 支持
- 新增 `update fs s3/rados info` 命令
- 挂载点信息新增 IP 字段
- 优化客户端重试逻辑，新增针对 `ESTORE_MAYBE_RETRY` 的重试支持
- 修复 TiKV scan 越界问题
- 修复 TiKV 事务丢失更新问题
- 修复批量删除块时超过最大值（1000）的问题
- 修复 compact chunk 使用旧文件长度的问题
- 修复 readdir 相关问题
- 修复页面文件系统树视图展示异常的问题
- 修复 WorkerSet 队列等待/运行延迟指标统计问题
- 修复 store 操作的已知问题
- 重构 MDS 内部 inode 和 partition 结构
- 使用 shard 优化 map，减小锁粒度，提升高并发性能
- 支持 batch mkdir/mknod/create/link/symlink 接口
- 支持 MDS 集群共享后端存储

**通用 (Common / General)**
- 支持 POSIX ioctl
- 支持自动清理日志
- 守护进程模式下支持重定向标准输入/输出日志路径
- 以 root 身份运行时，默认运行目录改为 `/var`
- 新增 block access 基准测试工具
- 新增 Rados 配置支持
- 重构项目目录结构及配置模块
- 支持 Open Telemetry 链路追踪
- 修复命令行帮助与日志文件中 flag 当前值不一致的问题
- 修复日志文件中 gflags 当前值显示错误的问题
- 修复 wrapper 销毁时的 coredump 问题

**监控 (Monitoring)**
- 新增 slice 指标
- 修复 in-flight 操作数指标统计错误的问题
- 重构 dingo 监控命令结构
- 结果响应中的参数改为 snake_case 命名
- 修复无监控目标时监控脚本抛出异常的问题
- `target_json.py` 新增 `mdsaddr` 参数支持
- 新增集群性能监控面板

## [5.0.0]

**客户端 (Client)**
- 客户端重新设计实现
- 客户端支持 FUSE 平滑升级
- 客户端新增监控面板
- 客户端支持缓存 dentry/inode/chunk 元数据
- 支持 S3 和 Ceph Rados 作为持久化存储

**缓存 (BlockCache)**
- 新增分布式缓存
- 缓存系统新增异步操作接口，提高 I/O 并发处理能力
- 缓存系统集成 `io_uring`，支持高效的文件读写操作，降低系统调用开销

**元数据服务 (MDS)**
- 元数据服务（MDS）彻底重构，完整支持 POSIX 语义
- 支持 Mono (单分区) 和 Hash (哈希分区) 两种元数据分布策略
- 支持分布式锁机制
- 支持故障恢复
- 支持文件系统级和目录级的配额
- 支持内部运行状态可视化功能，便于运维与调试
- 支持 chunk 的自动压缩与整理
- 支持文件系统元数据的备份与恢复功能
- 支持元数据节点的动态加入与退出
- 支持 dingo-store 作为后端元数据存储引擎
- 支持对分布式缓存节点的统一管理

**监控与可观测性 (Monitoring & Observability)**
- 支持集成 Grafana 和 Prometheus 进行监控
- 新增分布式缓存监控及新版元数据监控
- 新增追踪模块
