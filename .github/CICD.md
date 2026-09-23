# DingoFS CI/CD

dingofs 使用 GitHub Actions 与 GitHub Merge Queue 验证 `main` 和 `v5.[0-9]+` 维护分支（如 `v5.2`、`v5.10`）。`main` 额外运行 Jenkins 回归；维护分支暂不接入 Jenkins。发布流水接受上述分支 push 和 `v*` 标签 push；分支镜像与正式版本镜像使用不同标签，维护分支不发布 Python 包。

---

## 1. Workflows

| 文件 | 触发 | 做什么 |
|---|---|---|
| `.github/workflows/pr-check.yml` | 目标为 `main` 或 `v5.[0-9]+` 分支的 `pull_request` + `merge_group` | 普通 PR 的重检查全部跳过；merge group 顺序执行 `unit-test` → `build` → `e2e`，仅 `main` 的 merge group 并行运行 `jenkins-regression` |
| `.github/workflows/pr-source.yml` | 目标为 `main` 或 `v5.[0-9]+` 分支的 `pull_request_target` + `merge_group` | 可选的来源准入提示；`TRUSTED_SOURCE_ENABLED=false` 时跳过，不检出 PR 代码，不替代代码审核 |
| `.github/workflows/release.yml` | `push: branches:[main, 'v5.[0-9]+']` + `push: tags:['v*']` | build → docker-publish；wheels 仅 main/tag，pypi-publish 仅 tag。**不重测**，不得以发布成功替代合并前验证 |
| `.github/actions/build-release/` | composite action（被 pr-check `build` + release `build` 两处 `uses:` 内联）| `dingodatabase/dingo-eureka:rocky9-fs` container 内 Release cmake build，产 `dingofs.tar.gz` artifact。dingo-sdk install 走 `actions/cache`（同 unit-test，见 §8）。**逻辑复用走 composite 而非 `workflow_call`**——后者会让 required check 漂成锚不住的叶子名（见 §8）|

### Status Check 命名

`main` 和 `v5.[0-9]+` 维护分支的 branch protection 都使用以下 **3 个内联 job 裸名**作为 Required status checks：

- `unit-test` （pr-check.yml 内联 job）
- `build` （pr-check.yml 内联 job；构建逻辑 `uses: ./.github/actions/build-release` composite）
- `e2e` （pr-check.yml 内联 job）
- `main` 另外要求 `jenkins-regression`；维护分支暂不要求，也不会启动该 job。

为每条维护分支（例如 `v5.2`）单独配置对应保护规则与 Merge Queue。Actions 的 `v5.[0-9]+` 匹配 `v5.<数字>`，不匹配 `v5.2-debug`、`v5.2.0` 或 `v6.0`；不要直接将 Actions glob 复制到语法不同的保护规则中。workflow 不会创建分支、保护规则或队列，旧分支也不会因为主线更新而自动获得这份 workflow；本次不重命名或修改旧 `v5.1` 等分支。检查的 Expected source 均选择 GitHub Actions。

Jenkins 入口同时要求 `merge_group` 事件和 `github.event.merge_group.base_ref == 'refs/heads/main'`，并保留 `JENKINS_REGRESSION_ENABLED` 开关。维护分支不使用 Jenkins Environment，不需要扩展 trigger 或线上 Pipeline 的分支白名单；不要为了维护分支而关闭主线的 Jenkins 开关。

普通 PR 的 `unit-test`、`build`、`e2e`、`jenkins-regression` 均为 `skipped`，这只允许 PR 进入队列，不代表测试通过。队列针对合并候选实际执行该目标分支要求的检查。维护分支三项检查通过也不等同于完成 Jenkins 的回归覆盖，正式发布前仍需补齐候选提交的关键回归。

> ⚠️ 历史坑（务必遵守，实测踩过两段）：**required 的 `build` 绝不能用 `workflow_call` reusable 实现**。曾把 build 做成 `uses: _release-build.yml` 的 reusable caller，结果：
> 1. caller 用 `if` 跳过 → reusable 不实例化 → 叶子 check 永远 `Expected — Waiting for status to be reported` → 死锁；
> 2. 即便 caller 常驻，委托后 GitHub **只产出叶子名 `build / Build release artifacts`、不产出裸名 `build`** → required 裸 `build` 匹配不到任何 check → 一样 `Expected` 死锁。
>
> **根因**：`workflow_call` 把 required 的 check 名变成会漂移的叶子名（随 reusable 结构 / matrix 变）。**解法**：required 的 job 一律用普通**内联 job**（check 名 == job 名，永不漂移）；要复用构建逻辑就抽 **composite action**（在 caller 里内联执行，不改 check 名），**不要用 `workflow_call`**。

---

## 2. 本地调试（开发者）

提 PR 前本机跑一遍 e2e 验证：

```bash
# 默认：复用本机已装的 dingocli / uv 跟已拉的 docker images
bash .github/scripts/simulate-locally.sh

# 真实 CI 冷启模拟（重新 docker pull + apt + curl install）
PULL=1 INSTALL=1 bash .github/scripts/simulate-locally.sh

# 跑挂时保留 runtime 不 teardown 方便 debug
NO_TEARDOWN=1 bash .github/scripts/simulate-locally.sh

# 改 _lib/*.sh 后单独 verify 某个 helper
bash .github/scripts/_lib/preflight.sh
bash .github/scripts/_lib/install.sh
bash .github/scripts/_lib/glog-scan.sh
```

预期：119/119 pytest pass，~3-5 min（本机 9950X 实测 196s）。

**GHA hosted runner ≈ 本机**：`simulate-locally.sh` 跟 workflow yml 调用同一份 `_lib/*.sh` + `up.sh` + `deploy-mds-client.sh` + `down.sh`，byte-identical alignment，0 漂移。本机 pass = CI 大概率 pass。

---

## 3. Scripts 索引

`.github/scripts/` 下的所有 shell 脚本 + compose 文件：

| 文件 | 调用方 | 作用 |
|---|---|---|
| `docker-compose.yml` | `up.sh` | 1+1 dingo-store + minio compose 定义。**minio sha256 pinned + dingo-store `:latest`**（详 §5 Maintenance） |
| `up.sh` | workflow / sim | `docker compose up` → 等 minio + coord listen → 等 HEARTBEAT → `mc mb` 建 bucket |
| `deploy-mds-client.sh` | workflow / sim | 渲染 mds.conf → `dingo-mds-client CreateAllTable --mds_storage_dingodb_replica_num=1` → 起 dingo-mds → `dingo fs create` → mount dingo-client (FUSE) |
| `down.sh` | workflow / sim | `fusermount -uz` → kill PIDs → `docker compose down -v` → 清 RUNTIME_DIR |
| `simulate-locally.sh` | dev only | 本机按 pr-check.yml e2e job 同顺序跑同一份 shell（不是 act） |
| `get-image-digest.sh` | maintainer | 拉 `<image>:<tag>` 输出 sha256 digest（手工 bump 用）|
| `_lib/preflight.sh` | yml + sim | sysctl tune (`vm.overcommit_memory=1` + `vm.max_map_count=655360`) + THP madvise + disk/mem check |
| `_lib/install.sh` | yml + sim | apt fuse3 + dingocli (latest) + uv install |
| `_lib/glog-scan.sh` | yml + sim | 扫 dingo-client glog 找 4 类异步错误（`NoSuchBucket` / `Retry upload` / `CacheUnhealthy` / `Transport endpoint`）作 pytest 前的 fail-fast gate |

### 维护约定

- workflow yml + simulate-locally.sh 都 source `_lib/*.sh` —— byte-identical alignment，0 漂移
- 修 `_lib/*.sh` 不需要改 yml；workflow 跟 sim 自动跟进
- 修主 helpers（up / down / deploy-mds-client）需检查 simulate-locally.sh 调用方式是否一致

---

## 4. 流程图

### 4.1 PR 流程（pr-check.yml）

1. 开发者提交目标为 `main` 或 `v5.[0-9]+` 维护分支的 PR，触发 `pr-check.yml`。
2. `unit-test`、`build`、`e2e`、`jenkins-regression` 全部跳过；启用时，PR Source 单独执行来源准入提示。
3. 完成代码审核后加入目标分支的 Merge Queue。
4. 队列为合并候选触发真实检查，见 4.2。不要把 PR 阶段的 skipped 状态当作回归结果。

### 4.2 Merge Queue 流程

1. GitHub 将 PR 与目标分支及队列中前序改动组合成 merge group，生成独立的候选 SHA。
2. `merge_group` 触发 `unit-test`、`build`、`e2e`，后一个 job 依赖前一个成功。
3. 目标为 `main` 时，`jenkins-regression` 同时开始；目标为维护分支时跳过 Jenkins。
4. 对应 Required checks 全部成功后合并；失败则退出队列，PR 保持打开。
5. 合入 `main` 或 `v5.[0-9]+` 维护分支后，符合路径过滤的 push 触发分支镜像发布；正式版本仍通过 `v*` 标签发布。

**关键**：queue 出队跑的是 **rebased 新 SHA**——保证 main 上每个 commit 都被测过精确的 merge 后状态。

### 4.3 Release 流程（push main / maintenance branch / tag）

1. `main` 或 `v5.[0-9]+` 维护分支 push，或 `v*` 标签 push，触发 `release.yml`。
2. `build` 使用 `./.github/actions/build-release` 生成本次提交的 artifact；`docker-publish` 只消费同一 run 的 artifact。
3. Docker 发布到 `dingodatabase/dingofs`，标签规则如下。分支与 Git tag 分别按 `refs/heads/` 和 `refs/tags/` 判断，不混用。
4. `wheels` 仍仅在 main/tag 构建；`pypi-publish` 仍仅在 tag 发布。维护分支 push 不构建 wheel、不上传 PyPI。

| 触发 ref | Docker image tags |
|---|---|
| `refs/heads/main` | `latest`、`<7位SHA>` |
| `refs/heads/v5.2` | `v5.2-latest`、`v5.2-<7位SHA>` |
| `refs/heads/v5.3` | `v5.3-latest`、`v5.3-<7位SHA>` |
| `refs/tags/v5.2.0` | `v5.2.0` |
| `refs/tags/v5.2.0-rc.1` | `v5.2.0-rc.1` |

分支持续镜像中的 `-latest` 表示该维护分支最新构建，不代表正式发版；不会覆盖主线 `latest`。分支 push 仍遵守现有 `paths-ignore`，纯文档等被忽略的变更不触发发布；tag push 不受路径过滤影响。RC 标签的既有 Docker/PyPI 行为未在本次修改，打 tag 前仍需明确发布策略。

**为什么 release 不重测**：发布流水只负责构建和发布，依赖合并前的队列验证；必须先为维护分支配置保护与 Merge Queue。绕过队列合并或直接打 tag 不会自动补跑回归。

---

## 5. 依赖管理（日常 unpin / release pin 双形态）

dingo 系自家依赖（dingocli + dingo-store image）日常**不 pin**，跟随上游 latest；dingo 系外的第三方依赖（minio image / GHA actions）始终 pin sha256/commit 防供应链漂移。**打 release branch / tag 时两类都临时 pin**。

### 5.1 日常形态（main / feature 分支）

| 依赖 | 文件 | 策略 | 备注 |
|---|---|---|---|
| `dingocli` | `.github/scripts/_lib/install.sh` | **不 pin**：`curl .../releases/latest/download/dingo` | dingo 系自家工具，向后兼容由上游保证 |
| `dingodatabase/dingo-store` image | `.github/scripts/docker-compose.yml` | **不 pin**：`image: dingodatabase/dingo-store:latest` | 同组织，跟随上游迭代 |
| `minio/minio` image | `.github/scripts/docker-compose.yml` | **pin sha256**：`image: minio/minio@sha256:...` | 第三方供应链，pin 防漂移 |
| GHA actions | workflow yml | **pin commit hash**：`uses: foo@<sha>` | 同上，社区 action 防供应链投毒 |

**为什么 dingo 系日常不 pin**：dingofs e2e 测试要验证的就是"客户端跟最新 dingo-store / dingocli 的兼容性"，pin 反而掩盖 dingo 系自身的 regression。pin 后每次上游发版还要手工 bump，运维成本 > 安全收益。

### 5.2 Release Pinning Checklist（打 tag / 开 release branch 前必做）

```
□ 1. 跑 `bash .github/scripts/get-image-digest.sh dingodatabase/dingo-store latest`
     → 拿当前 dingo-store image digest，写入 docker-compose.yml coordinator/store image 字段
□ 2. 确认 dingocli 当前 stable release tag (如 v5.1.0)，跑：
     curl -fsSL ".../releases/download/v5.1.0/dingo" | sha256sum
     → 把 tag + sha256 写回 _lib/install.sh (加 DINGOCLI_TAG + DINGOCLI_SHA256 + sha256sum -c 三行)
□ 3. 本机 `bash .github/scripts/simulate-locally.sh` 跑 119/119 pass，确认 pin 形态没破东西
□ 4. 改动落到维护分支（例如 `v5.2`）或直接打 tag 的 commit
□ 5. push 维护分支 → 发布分支 Docker 镜像；push v* tag → 发布版本 Docker 镜像和 PyPI 包
□ 6. main 分支保持 unpin 形态不动（release branch/tag 是独立分叉，不 merge 回主干）
```

**为什么 release 要 pin**：

- 用户报告 v0.x.x 出 bug 时，maintainer 要能精确 checkout 该 tag 复现，pin 是唯一能保证"复现环境跟当时发版一致"的手段
- 半年后回看老 release，dingo-store latest 早飘到 v3.0 完全跑不动当时的 v0.5 release，pin 防退化
- 日常 main 不 pin 是为了跟进上游 + e2e 覆盖兼容性；release 是 frozen artifact，恰恰相反

---

## 6. 故障排查

CI 红 → 下载 logs artifact → 解压看：

```bash
gh run download <run-id> --repo dingodb/dingofs --name ci-logs-<run-id>
tar -xzf ci-logs/*.tgz  # 各组件 glog
```

artifact 内容：

```
ci-logs/
├── coord.stdout.log         dingo-coordinator container docker logs
├── store.stdout.log         dingo-store container docker logs
├── minio.log                minio container docker logs
├── coord.glog.tgz           容器内 dingo-coordinator glog（HEARTBEAT 等）
├── store.glog.tgz           同上 store
└── dingofs-runtime.tgz      宿主 dingo-mds + dingo-client 的 conf + glog
                             (mds.conf / dingo-mds.INFO / dingo-client.INFO)
```

### 典型错误

| 错误 | 根因 | 修法 |
|---|---|---|
| `Not enough stores for create region` | dingo-mds gflag `--mds_storage_dingodb_replica_num` 默认 3，单 store stack 不够 | 已修：`deploy-mds-client.sh` 启动加 `--mds_storage_dingodb_replica_num=1`（上游 dingo-mds bug，等 fix 后可删该 flag）|
| `NoSuchBucket` | minio bucket 没建（`mc mb` 失败 / minio 没起）| up.sh 失败重跑；检查 minio container 状态 |
| `Transport endpoint not connected` | dingo-mds / dingo-store 连接断 / dingo-client 进程崩了 | 看 client glog 找堆栈；看 coord glog 找 raft leader |
| `RangeError: Invalid string length` | actions/download-artifact 跨 workflow 撞 Node 2GB 限制（artifact 2.5GB）| 已修：用 `actions/download-artifact@v4` 同 run 取，不跨 workflow |
| pytest hang 25min timeout | 通常是 dingo-client 后台异步 retry 死循环（`NoSuchBucket` / `Retry upload`）| `_lib/glog-scan.sh` 在 pytest 前扫，命中提前红，省 20min 排错时间 |

### 失败分阶段处理

**普通 PR 阶段**：重检查只建立 skipped 状态，不执行编译或回归。更新 PR 会刷新这组状态，但编译、单测、e2e 和 Jenkins 的实际失败要在队列阶段处理。

**Merge Queue 阶段失败**（queue 出队后 unit/build/e2e 任一红）：
- queue UI 显示 PR ✗
- PR 自动从 queue 踢出，PR 留 open
- 作者重新 push (可能因 rebase 跟其他 PR 冲突) → 重新走 PR check → 重新入 queue

**Release 阶段失败**（build / docker-publish / pypi-publish 任一红）：
- 不发包，**main 状态不受影响**（已经 merged）
- maintainer 收 GitHub email + Actions UI 红色
- `build` 红：罕见（main 已被 queue 测过 build）；通常是基础设施抖（dingo-eureka 镜像拉不到）→ 重跑
- `docker-publish` 红：检查 DOCKERHUB_USERNAME/TOKEN secret；改后重跑 release.yml
- `pypi-publish` 红：检查 PYPI_API_TOKEN；不要重 tag（pypi 拒重传），直接重跑 release.yml

---

## 7. 维护人员 Checklist

### 日常 bump 流程

```
□ 1. 改 _lib/install.sh / docker-compose.yml / workflow yml (按 §5 规则)
□ 2. 本机 simulate-locally.sh 验证
□ 3. 开 PR
□ 4. pr-check 全绿 → merge queue → main green
```

### 打 release tag

```
□ 1. 按 §5.2 Release Pinning Checklist 走 6 步
□ 2. push tag → release.yml 自动发 docker + pypi
□ 3. release.yml 红时按 §6 失败分阶段处理
```

---

## 8. 设计决策（why）

- **为什么使用独立入口与 composite action**：PR Check 负责候选验证，PR Source 负责可选来源准入，Release 负责发布。构建逻辑通过 composite action 复用，不使用会改变 Required check 名称的 `workflow_call` caller。
- **为什么 release 不重测 unit/build/e2e**：merge queue 已用 rebased SHA gate 过，重测是浪费 + 阻塞 publish。release.yml 信任 queue 保证。
- **为什么普通 PR 跳过重检查**：`unit-test`、`build`、`e2e` 只在 merge group 的合并候选上运行，避免 PR 和队列重复构建；主线 Jenkins 同样只在队列运行。代价是审阅 PR 时没有本轮回归结果，绕过队列直接合并也就绕过了这些验证。
- **为什么 `build` 是普通内联 job + composite action，而不用 `workflow_call`**：required check 必须名字稳定，而 `workflow_call` reusable 的 check 会漂成叶子名 `build / <job>`，且 caller 被 skip 时还卡 `Expected`（实测两次死锁，详 §1 ⚠️）。改用普通内联 `build` job：check 名就是 `build`；`if: github.event_name == 'merge_group'` 跳过时直接报 `skipped`（跟 `e2e` 同机制，满足 required 不挡），merge_group 才真跑、`success` 才放行。构建逻辑（sdk + dingofs Release 编译 + 产 `dingofs.tar.gz`）抽到 composite action 给 pr-check `build` 与 release `build` 两处 `uses:` 复用——composite 在 caller 内联执行,既复用逻辑又不引入会漂移的叶子 check 名。**这是整套"PR skip / 队列真跑 / 三个都 required"设计能跑通的命门**：required 的 job 必须是内联 job。
- **为什么缓存 dingo-sdk install（`actions/cache`）**：`unit-test` 与 `build` 都需要 SDK，共享相同 cache key 和 `.cache-complete` 哨兵，命中时复用已安装的 SDK。构建配方、Eureka 镜像或 SDK 提交改变会使 key 失效。普通 PR 不运行这些构建，也不会读取或填充这份缓存；composite action 的缓存保存仍在 job 结束时执行。
- **cache key 的四段构成（为什么不只靠外部 head + sentinel）**：`dingo-sdk-v1-<构建配方指纹>-<eureka 镜像 ID>-<dingo-sdk main HEAD SHA>`。
  - `<dingo-sdk main SHA>` + `<eureka 镜像 ID>`：**外部输入**变了就重编（ABI 不会拿旧 sdk 配新 eureka）。
  - `<构建配方指纹>` = `hashFiles('.github/scripts/_lib/build-dingo-sdk.sh')`：**本地"怎么编"**(cmake flags / 编译命令)变了就重编。sdk 的 clone+cmake+make 抽到这个**单一真相源脚本**（unit-test 和 build 都 `source` 它，不会两边 drift），改它 → hash 变 → key 自动失效。**这是关键**：只锚外部 head + `.cache-complete` sentinel 的话，改了配方但 key 不变，sentinel 会"自信地"命中、复用配方过时的 sdk；指纹堵上这个洞，也免去手动 bump。
  - `v1`：保留的**手动版本位**，留作"配方没变但想强制刷缓存"（如缓存损坏）的应急杠杆。
- **为什么没有 nightly main 健康检查**：merge queue 已保证 main 上每个 commit 测过；nightly 防"依赖漂移 / 镜像更新"的兜底场景按需独立加，不强制属于本设计核心。
- **为什么 dingocli + dingo-store image 日常不 pin**：dingofs e2e 测试要验证的就是"客户端跟最新 dingo-store / dingocli 的兼容性"，pin 反而掩盖 dingo 系自身的 regression；release 时 pin 是为可重现（详 §5）。
- **为什么必须 GitHub Merge Queue（org-only feature）**：merge_group event 配 rebased SHA gate 是 GitHub 原生最干净的 race 防护——broken 状态进不去 main。Merge Queue 是 organization 仓 only，个人 fork 用不了（`mergeQueue=null + isInOrganization=false` 实测），所以本设计的落地仓必须是 organization 账户（dingodb/dingofs）。
