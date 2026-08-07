# readdir 的 `.`/`..` 合成目录项在客户端 VFSImpl 层注入

FUSE 内核不会为文件系统自动合成 `.` 和 `..`，POSIX 要求 readdir 返回它们。DingoFS 选择在**客户端 VFSImpl 层**合成：`.`/`..` 对每个目录注入，`.stats`/`.trash` 对 FUSE 可见根注入，全部合成项与真实 dentry 共享同一个位置制 cookie 空间（流中位置 p 的 cookie 为 p+1），委托给 MetaSystem 前将 offset 扣减合成项数 S。MDS 的 ReadDir RPC 保持不变，不返回合成项。

**Considered Options**

- **MDS 服务端注入**：被拒绝。`.`/`..` 不是存储事实；服务端合成会污染 `last_name` 分页游标，并影响 ReadDir RPC 的其他消费者。
- **DirIterator 固化**：被拒绝。`Fetch()` 用 `entries_.back().name` 作为下一页的 `last_name`，若批边界落在 `..` 上，MDS 从 `".."` 之后扫描会跳过名字在 `(".", "..")` 区间的真实文件（如 `.a`）。
- **FUSE 层注入**：被拒绝。`..` 的 attr 需要额外发起 GetAttr 调用链，且与 VFSImpl 已有的 `.stats`/`.trash` 注入分处两层，cookie 空间被撕裂。
- **MetaSystem 层注入**：被拒绝。warmup_manager 直连 `meta_system->ReadDir` 递归收集子目录，注入 `..` 会导致无限递归。

**Consequences**

- 合成项的 uid/gid 不能走真实项的 `TranslateAttrToLocal` 包装：`VFSImpl::GetAttr` 内部已翻译，二次翻译会产生垃圾 id。合成项走原始 handler，真实项走 wrapped handler。
- `..` 的上报 ino 遵循固定规则：挂载根自指 `kRootIno`；`.trash` 虚拟目录指向 `kRootIno`；`parents` 为空时防御性自指；`parents[0] == mount_root_ino_` 时改写回 `kRootIno`（TranslateIno 的反向）。
- 目录被 rmdir 但仍被持有读取时（GetAttr 返回 NotExist/NotFound），整个 readdir 流读为空，不委托 MetaSystem。

**术语表**

- **Dentry（真实目录项）**：存储在 MDS 后端、由扫描返回的目录项，有真实 inode 绑定。`.`、`..`、`.stats`、`.trash` 都不是 dentry。
- **合成目录项（Synthesized Entry）**：不由 MDS 存储、由客户端在读目录流中临时注入的目录项：每个目录的 `.` 和 `..`，以及根目录的 `.stats`、`.trash`。
- **Cookie（readdir offset）**：内核 readdir/readdirplus 的位置凭证，按目录项在流中的位置顺序生成（位置 p 的 cookie 为 p+1）。合成目录项与 dentry 共享同一个 cookie 空间。
- **DirIterator**：客户端侧某个已打开目录句柄（fh）的 MDS dentry 游标，按 `last_name` 分页拉取，仅包含真实 dentry。
- **挂载根（Mount Root）**：FUSE 命名空间中的根 ino（`kRootIno`）。子目录挂载时它翻译到底层的 `mount_root_ino_`；反向地，任何等于 `mount_root_ino_` 的真实 ino 上报给内核时必须改写回 `kRootIno`。挂载根的 `..` 自指 `kRootIno`。
