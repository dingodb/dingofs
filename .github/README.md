# DingoFS CI/CD

dingofs 的 CI/CD 系统：**3 个 GitHub Actions workflow** 配 **GitHub Merge Queue** 守护 main 分支永远绿，发布时自动产 docker image + pypi wheel。

---

## 1. Workflows

| 文件 | 触发 | 做什么 |
|---|---|---|
| `.github/workflows/pr-check.yml` | `pull_request` + `merge_group` | 3 个 status check 串联：`unit-test` → `build / release-build` → `e2e`。**任一红 → 阻 merge button**。merge_group event 是 Merge Queue 出队 gate（rebased SHA 重测）|
| `.github/workflows/release.yml` | `push: branches:[main]` + `push: tags:['v*']` | 发布流水：build → docker-publish (always) + wheels → pypi-publish (tag only)。**不重测**（信任 merge queue 已守门）|
| `.github/workflows/_release-build.yml` | `workflow_call:` only | 被 pr-check / release 复用——`dingodatabase/dingo-eureka:rocky9-fs` container 内 Release cmake build，产 `dingofs.tar.gz` artifact |

### Status Check 命名

dingodb maintainer 配 branch protection required status checks 时填这 3 个：

- `unit-test` （pr-check.yml 内联 job）
- `build / release-build` （pr-check.yml 嵌套调用 `_release-build.yml`）
- `e2e` （pr-check.yml 内联 job）

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

```
开发者 push 到 PR 分支
        ↓
pr-check.yml 触发（pull_request event）
        ↓
┌─────────────────────────────────────────────┐
│  job: unit-test  (内联)                       │
│        ↓ pass                                │
│  job: build      (uses _release-build.yml)   │
│        ↓ pass                                │
│  job: e2e        (内联：起 stack + pytest 119)│
│        ↓ pass                                │
│  3 个 status check 全绿                       │
└─────────────────────────────────────────────┘
        ↓
PR mergeable，reviewer 点 "Add to merge queue"
        ↓
PR 进入 Merge Queue
```

### 4.2 Merge Queue 流程

```
Queue: [PR-A, PR-B, PR-C, ...]
        ↓
GitHub 自动出队 PR-A
        ↓
自动 rebase PR-A 到当前 main HEAD
        ↓
触发 pr-check.yml（merge_group event，跑 rebased 新 SHA）
        ↓
   ┌───┴───┐
  pass    fail
   ↓        ↓
merge   踢出 queue
入 main   ↓
   ↓     PR 留 open，开发者修
push event → release.yml 触发（见 4.3）
```

**关键**：queue 出队跑的是 **rebased 新 SHA**——保证 main 上每个 commit 都被测过精确的 merge 后状态。

### 4.3 Release 流程（push main / tag）

```
PR 通过 queue → merge 入 main → push event 触发
       OR
开发者 push tag v* → release.yml 触发
        ↓
┌─────────────────────────────────────────────┐
│  job: build  (uses _release-build.yml)      │
│        ↓                                     │
│  ┌─ Docker 链 ─────────────────────┐        │
│  │ job: docker-publish (内联)      │        │
│  │   needs: build                  │        │
│  │   → image push (always)         │        │
│  └─────────────────────────────────┘        │
│  ┌─ Pypi 链（独立并行 Docker 链）─┐          │
│  │ job: wheels (内联 cibuildwheel)│          │
│  │   → dingofs_whl artifact       │          │
│  │ job: pypi-publish (内联 twine) │          │
│  │   needs: wheels                │          │
│  │   if: startsWith tag           │          │
│  │   → wheel push (tag only)      │          │
│  └────────────────────────────────┘          │
└─────────────────────────────────────────────┘
```

**为什么 release 不重测**：merge queue 已保证入 main 的每个 commit 都被测过精确的 merge 后状态。release 信任这个保证，只负责 build artifact 给 docker/pypi。

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
□ 4. 改动落到 release branch (`release/v0.x`) 或直接打 tag 的 commit
□ 5. push release branch / tag → release.yml 触发 → docker / pypi 发包
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

**PR 阶段失败**（unit-test / build / e2e 任一红）：
- 显示在 PR Checks tab
- branch protection 阻 merge button
- 开发者 push fix → 自动重跑 pr-check
- concurrency 自动 cancel 旧 run

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

- **为什么 3 个 workflow 而不是 1 个大的或 6 个小的**：3 workflow 对应 PR / Release 两个事件入口 + 1 个共享引擎（`_release-build.yml` 被 pr-check 跟 release 两处复用）。原本设计有 6+ 个独立 workflow，但单点 use 的 sub-workflow 都已内联到唯一 caller——**reuse 才抽独立文件，单用就 inline**。
- **为什么 release 不重测 unit/build/e2e**：merge queue 已用 rebased SHA gate 过，重测是浪费 + 阻塞 publish。release.yml 信任 queue 保证。
- **为什么没有 nightly main 健康检查**：merge queue 已保证 main 上每个 commit 测过；nightly 防"依赖漂移 / 镜像更新"的兜底场景按需独立加，不强制属于本设计核心。
- **为什么 dingocli + dingo-store image 日常不 pin**：dingofs e2e 测试要验证的就是"客户端跟最新 dingo-store / dingocli 的兼容性"，pin 反而掩盖 dingo 系自身的 regression；release 时 pin 是为可重现（详 §5）。
- **为什么必须 GitHub Merge Queue（org-only feature）**：merge_group event 配 rebased SHA gate 是 GitHub 原生最干净的 race 防护——broken 状态进不去 main。Merge Queue 是 organization 仓 only，个人 fork 用不了（`mergeQueue=null + isInOrganization=false` 实测），所以本设计的落地仓必须是 organization 账户（dingodb/dingofs）。
- **为什么 unit-test + e2e 内联在 pr-check.yml，而 release-build 抽成独立**：unit-test + e2e 只被 pr-check 一处用（单点）→ 内联。release-build 被 pr-check + release 两处用（双点）→ 抽 `_release-build.yml`。下划线前缀标记"workflow_call only，不被开发者直接触发"。
