# Jenkins 回归门禁 Job 配置手册

本文用于配置 DingoFS Merge Queue 的 Jenkins 回归 Job。配置 Jenkins 本身不要求代码已经提交；真正的 GitHub Actions → Jenkins → Merge Queue 全链路，需要相关 workflow 和 trigger 进入受保护的 `main` 后才能验证。

## 1. 固定配置

| 配置 | 值 |
|---|---|
| Jenkins URL | `https://lapping-diagnoses-unbeaten.ngrok-free.dev/` |
| Jenkins Controller | `2.504.1` |
| Pipeline Job | `dingofs-workflow-PR-127` |
| Agent label | `dingofs-regression` |
| Lockable Resource | `dingofs-regression-env` |
| Jenkins 服务账号 | `dingofs` |
| Pipeline 文件 | `scripts/jenkins/dingofs-merge-regression.Jenkinsfile` |
| DTT 回归超时 | 90 分钟 |
| GitHub trigger 等待上限 | 260 分钟 |
| GitHub Actions Job 硬超时 | 270 分钟 |

不要把仓库 URL、部署目录或测试目录做成 GitHub 可以传入的 Job 参数。

## 2. 安装插件

进入：

```text
Manage Jenkins → Plugins
```

安装或确认以下插件：

| 插件 | 用途 |
|---|---|
| Pipeline | 运行 Declarative Pipeline |
| Lockable Resources | 串行使用唯一回归环境 |
| Matrix Authorization Strategy | 将服务账号权限限制到指定 Job |
| Timestamper | Jenkinsfile 使用了 `timestamps()` |

说明：

- 当前 Controller 是 Jenkins `2.504.1`，应使用 Plugin Manager 提供的兼容版本；不要手工上传要求更高 Jenkins Core 的当前最新版。
- Jenkinsfile 在 `sh` 中调用 agent 的 CLI `git`，所以 Git plugin 不是硬依赖，但 agent 必须安装 `git` 命令。
- Pipeline 需要 Declarative Pipeline 能力；安装 Jenkins 的 Pipeline 聚合插件时通常会自动安装。
- 如果 Jenkins 提示必须重启，应在没有其他构建运行时重启。

## 3. 配置 Jenkins 公网地址

进入：

```text
Manage Jenkins → System → Jenkins Location
```

设置：

```text
Jenkins URL=https://lapping-diagnoses-unbeaten.ngrok-free.dev/
```

ngrok tunnel 必须覆盖 Jenkins 排队、构建、约一小时回归以及环境清理的完整时间。不要在运行中的回归期间关闭或更换 tunnel。

## 4. 配置执行节点

进入：

```text
Manage Jenkins → Nodes → 选择实际运行回归的节点 → Configure
```

部分 Jenkins UI 显示为：

```text
Manage Jenkins → Manage Nodes and Clouds
```

### 4.1 配置 label

在 `Labels` 中加入：

```text
dingofs-regression
```

如果节点已有其他 label，使用空格分隔，不要删除原有值。

### 4.2 确认 Agent 身份与路径

本 Job 不需要额外配置 node 环境变量。保存 label 后确认：

- 节点状态为 `Online`；
- agent 进程以操作系统用户 `jenkins` 运行；
- Pipeline 能访问固定的 `/home/jenkins/...` 和 `/mnt/disk1/...` 路径；

### 4.3 Agent 只读预检

以下命令只做检查，不部署组件、不删除文件。请以实际运行 agent 的 `jenkins` 用户执行：

```bash
set -euo pipefail

for command_name in bash git docker jq ss; do
  command -v "${command_name}"
done

docker info >/dev/null
test -x /home/jenkins/miniforge3/bin/python
test -x /home/jenkins/.dingo/bin/dingo
test -x /home/jenkins/.local/bin/dtt
/home/jenkins/.dingo/bin/dingo cluster ls

components=/home/jenkins/.dingo/components
test -d "${components}"
test ! -L "${components}"
test "$(realpath -e "${components}")" = "${components}"
```

Pipeline 直接执行 127 上受管理员维护的 `/home/jenkins/.local/bin/dtt`，不会
clone、更新或构建 testsuite，也不会获取或构建 testsuite 镜像。四个候选组件的
`.../main` 目录如尚不存在，由首次受保护 Pipeline 在校验
`/home/jenkins/.dingo/components` 后创建。

### 4.4 现有 127 部署脚本的关系

127 原有的 `/home/jenkins/jenkins_workdir/script-main` 只作为拓扑和部署机制参考。
新 Job 不直接执行其中的 `bootstrap-build-dingofs.sh` 或 `sync_topology.sh`：前者会
对 `/home/jenkins/code/dingofs-main` 执行 `git pull`，后者会原地修改共享 topology，
都不能证明部署的是 merge-group 精确 SHA。

受保护 Jenkinsfile 会从当前 workspace 构建唯一候选镜像，在本次 build 私有目录生成
同等拓扑，再调用现有 `/home/jenkins/.dingo/bin/dingo` 的
`cluster deploy -k --local`。固定 cluster name 仍为 `main-dingofs`。每次回归会先销毁
同名旧集群、重新部署，测试结束后再次销毁。因此原有部署 Job 必须停用，或改为获取同一个
`dingofs-regression-env` 锁；不能与本 Job 并发运行。

本机 DTT 的 `env_127` 应使用 127 的 MDS `6900-6902`、Executor
`18765/13307` 和专用 bucket `dingofs-jenkins-127`。Pipeline 只调用
`dtt smoke --env env_127`，不会检查、创建或修改 DTT 环境定义。Pipeline 不会创建、清空或验证 MinIO bucket；bucket 必须由 DTT 维护者预先准备，并由管理员人工定时清理。

## 5. 创建共享环境锁

进入：

```text
Manage Jenkins → Lockable Resources → Resources → Add Resource
```

填写：

```text
Name: dingofs-regression-env
Description: DingoFS shared regression environment
Labels: 留空
Reserved by: 留空
```

点击 `Save`。

注意：

- `dingofs-regression-env` 是资源名称，不是 node label。
- 正常状态应为 `Free`，不要提前 Reserve。
- 只有同样申请这个资源的 Job 才会被串行化。任何会修改四个组件、`main-dingofs`、client log 或执行 DTT 的旧 Job，都必须停用或使用同一个锁。

## 6. 创建 Jenkins 服务账号

如果 Jenkins 使用自带用户库，进入：

```text
Manage Jenkins → Users → Create User
```

创建：

```text
Username: dingofs
```

如果 Jenkins 使用 LDAP、OIDC 或其他 Security Realm，应在对应身份系统中创建账号，不要为了这个 Job 更换现有 Security Realm。

先完成权限配置，再生成 API token。

## 7. 配置 Project-based Matrix 权限

进入：

```text
Manage Jenkins → Security
```

旧版 UI 可能显示为：

```text
Manage Jenkins → Configure Global Security
```

在 `Authorization` 中选择：

```text
Project-based Matrix Authorization Strategy
```

按以下顺序配置，避免管理员被锁出：

1. 保留当前管理员或管理员组的 `Overall/Administer`。
2. 当前管理员浏览器会话不要退出。
3. 把 `dingofs` 作为 User 加入权限矩阵。
4. 全局只授予 `Overall/Read`。
5. `Anonymous` 不授予权限。
6. 保存后用另一个管理员窗口确认仍可进入 Jenkins。

还要检查全局 `authenticated` 行以及服务账号所属组。如果这些主体已经拥有全局 `Job/Build`、`Job/Configure` 等权限，服务账号仍会继承，无法真正限制到单个 Job。Matrix 权限是叠加授权，不能在 Job 层使用“拒绝”抵消全局权限。

## 8. 创建 Pipeline Job

返回 Jenkins Dashboard，点击：

```text
New Item
```

填写：

```text
Item name: dingofs-workflow-PR-127
Type: Pipeline
```

点击 `OK`。

### 8.1 General

描述可以填写：

```text
DingoFS merge-queue shared-environment regression gate
```

不要启用以下配置：

- 不要手工勾选 `This project is parameterized`；
- 不要配置 Build Triggers；
- 不要启用 `Trigger builds remotely`；
- 不要配置单独的远程 build token；
- 不要在 UI 中重复配置并发、超时或构建保留数量。

参数、并发限制和最近 30 次构建保留策略均由 Jenkinsfile 定义。

### 8.2 Job 级权限

勾选：

```text
Enable project-based security
```

如果显示权限继承选项，选择：

```text
Do not inherit permissions
```

添加用户 `dingofs`，只授予：

```text
Job → Read
Job → Build
Job → Cancel
```

不要授予：

```text
Job/Configure
Job/Delete
Job/Create
Job/Workspace
Credentials/*
Overall/Administer
```

`Job/Cancel` 用于 GitHub run 被取消或超时时撤销 queue item、停止运行中的 build。

### 8.3 Pipeline script

在页面底部的 `Pipeline` 区域设置：

```text
Definition: Pipeline script
Use Groovy Sandbox: 勾选
```

禁止选择：

```text
Pipeline script from SCM
```

把以下文件全文复制到 `Script` 输入框：

```text
/tmp/dingofs-jenkins-regression-gate/scripts/jenkins/dingofs-merge-regression.Jenkinsfile
```

点击 `Save`。

Job 的 `Configure` 权限只能给管理员。GitHub 服务账号不能修改 Pipeline script。被测 merge-group 只作为构建输入，不能控制部署或清理逻辑。

## 9. 第一次无副作用验证

Job 保存后，第一次通常显示 `Build Now`，直接点击。如果已经显示 `Build with Parameters`，则保持所有参数为空并执行。

进入：

```text
dingofs-workflow-PR-127 → #1 → Console Output
```

预期结果：

```text
Stage "Validate request"
GIT_SHA must be a full 40-character commit SHA
Finished: FAILURE
```

这个 `FAILURE` 表示验证成功，因为它证明：

- Jenkins 成功解析了完整 Declarative Pipeline；
- `dingofs-regression` 节点能够被分配；
- Pipeline 参数已经注册；
- 没有执行 checkout；
- 没有获取共享环境锁；
- 没有替换组件或删除回归数据。

第一次执行后，Job 页面应显示 `Build with Parameters`。

检查共享环境没有被标记为异常：

```bash
test ! -e /home/jenkins/.cache/dingofs-ci/regression-unhealthy
```

并在 Lockable Resources 页面确认 `dingofs-regression-env` 仍为 `Free`。

## 10. 第二次安全参数验证

为确认 5 个参数和两个 node 环境变量已生效，可以执行一次必然不会进入构建阶段的参数化构建：

```text
GIT_REF=refs/heads/gh-readonly-queue/main/config-check-do-not-create
GIT_SHA=0000000000000000000000000000000000000000
GITHUB_RUN_ID=1
GITHUB_REPOSITORY=dingodb/dingofs
GITHUB_SERVER_URL=https://github.com
```

预期：

- `Validate request` 通过；
- 在 `Checkout exact merge group` 的 `git fetch` 阶段失败；
- 不进入 `Build exact SHA`；
- 不获取共享环境锁；
- 不修改共享回归环境。

即使远端意外存在这个 ref，实际 commit SHA 也不会等于全零 SHA，因此仍会在构建前失败。

## 11. 生成 API Token

使用 `dingofs` 登录 Jenkins，进入：

```text
右上角用户名 → Security → API Token → Add new Token
```

也可以直接访问：

```text
https://lapping-diagnoses-unbeaten.ngrok-free.dev/me/security
```

Token 名称建议：

```text
github-actions-dingofs
```

点击 `Generate` 并立即复制。Jenkins 只显示一次。

不要使用管理员 token，也不要把 token 写入 Job 参数、Pipeline script、仓库文件或命令历史。

## 12. API 权限验证

在安全终端执行：

```bash
JENKINS_URL=https://lapping-diagnoses-unbeaten.ngrok-free.dev
JENKINS_USER=dingofs

read -rsp 'Jenkins API token: ' JENKINS_API_TOKEN
echo

base=${JENKINS_URL%/}
auth="${JENKINS_USER}:${JENKINS_API_TOKEN}"

curl -fsS --user "${auth}" \
  "${base}/whoAmI/api/json" |
  jq '{authenticated,name}'

curl -fsS --user "${auth}" \
  "${base}/job/dingofs-workflow-PR-127/api/json?tree=name,url,buildable" |
  jq .
```

预期账号输出：

```json
{
  "authenticated": true,
  "name": "dingofs"
}
```

Job 应返回：

```json
{
  "name": "dingofs-workflow-PR-127",
  "buildable": true
}
```

继续测试 `Job/Build` 权限：

```bash
curl --silent --show-error \
  --user "${auth}" \
  --request POST \
  --dump-header - \
  --output /dev/null \
  --write-out '\nHTTP %{http_code}\n' \
  --data-urlencode 'GIT_REF=invalid' \
  --data-urlencode 'GIT_SHA=invalid' \
  --data-urlencode 'GITHUB_RUN_ID=1' \
  --data-urlencode 'GITHUB_REPOSITORY=dingodb/dingofs' \
  --data-urlencode 'GITHUB_SERVER_URL=https://github.com' \
  "${base}/job/dingofs-workflow-PR-127/buildWithParameters"

unset JENKINS_API_TOKEN auth
```

预期：

- HTTP 通常为 `201`，至少应为 `2xx`；
- 响应头包含 `Location: .../queue/item/<数字>/`；
- 新构建在 `Validate request` 以非法 SHA 失败；
- 不修改共享回归环境。

不要给 curl 添加 `-L`，否则可能掩盖用于定位 queue item 的 `Location` 响应头。

Jenkins API token 使用 Basic Auth 时不需要 crumb。CSRF 必须保持开启；如果返回 403，优先检查 token 和 Matrix 权限，不要关闭 CSRF。

## 13. GitHub 侧后续配置

以下操作都在 `dingodb/dingofs` 或 `dingodb` 组织的 GitHub 设置页面完成，
不能通过复制 Jenkinsfile 自动完成。

### 13.1 Repository Variables

进入：

```text
dingodb/dingofs → Settings → Secrets and variables → Actions → Variables
```

创建：

```text
JENKINS_URL=https://lapping-diagnoses-unbeaten.ngrok-free.dev
JENKINS_JOB_PATH=dingofs-workflow-PR-127
```

如需临时暂停 PR 来源检查和 Jenkins 回归，同时保留 Ruleset 中的 Required checks，
再创建以下 Repository Variables：

```text
TRUSTED_SOURCE_ENABLED=false
JENKINS_REGRESSION_ENABLED=false
```

`false` 必须为小写字符串。此时 `trusted-source` 和 `jenkins-regression` 会被 GitHub
标记为 skipped，不会阻止合并，也不会触发 Jenkins。恢复时将对应变量改为 `true`
或删除变量；变量不存在时 workflow 默认启用对应 job。

### 13.2 GitHub Environment 与 Secrets

进入：

```text
dingodb/dingofs → Settings → Environments → New environment
```

创建并配置：

```text
Name: jenkins-regression
Required reviewers: 不配置
Deployment branch pattern: gh-readonly-queue/main/*
```

只在该 Environment 中保存：

```text
JENKINS_USER=dingofs
JENKINS_API_TOKEN=<服务账号 API token>
```

不要创建同名 Repository Secrets。

### 13.3 普通 PR 与 Merge Queue 触发语义

本节以 `dingodb` 不是 GitHub Enterprise 为前提。Evaluate mode 在当前组织不可用，
因此不能先用 Ruleset Insights 观察再上线；必须先以 `Disabled` 保存完整规则，
完成静态核对后切换为 `Active`，再用专门的 fork 测试 PR 验收。

workflow 已经实现触发语义，不需要配置 webhook：

- 普通 PR 触发 `pull_request`，只有 `unit-test` 真正执行；
- `build`、`e2e` 和 `jenkins-regression` 在普通 PR 上为 skipped；
- PR 审批完成并加入 Merge Queue 后，GitHub 创建
  `refs/heads/gh-readonly-queue/main/*` 临时 ref，并发送独立的
  `merge_group` 事件；
- `merge_group` 对重新合并后的 SHA 执行：

```text
unit-test → build → e2e → jenkins-regression → Jenkins Job
```

`jenkins-regression` 会等待远端 Job 结束。Jenkins 返回 `SUCCESS` 时该 check
成功；Jenkins 返回 `FAILURE`、`UNSTABLE`、`ABORTED`、未知结果，或者 API
调用、排队、轮询超时，都会使 check 失败。只有把这个 check 配成 Required，
Jenkins 失败才会阻止合并。

需要人工完成的部分只有 GitHub 外部配置：

1. 先把 `.github/workflows/pr-check.yml`、`.github/workflows/pr-source.yml`、
   `.github/scripts/trigger-jenkins.sh` 及其运行依赖合入 `main`；
2. 按 13.1、13.2 配置 Repository Variables、Environment 和 secrets；
3. 按 13.4 创建仓库级 Ruleset，初始状态设为 `Disabled`；
4. 确认 Jenkins Job、127 agent、共享锁和公网 URL 全部可用；
5. 把 Ruleset 切换为 `Active` 后，使用专门的 fork PR 做真实验收；
6. 出现问题时立即把 Ruleset 切回 `Disabled`，停止新的合并操作，修复后重新验收。

### 13.4 Repository Ruleset：保护 main

此规则必须建在仓库级，因为 GitHub 的 `Require merge queue` 不能配置在组织级
Ruleset。

进入：

```text
dingodb/dingofs → Settings → Rulesets → Rulesets
→ New ruleset → New branch ruleset
```

如果已经存在保护 `main` 的 Branch ruleset，优先编辑现有规则，不要创建一套
互相重叠且难以排查的规则。

按以下值创建：

```text
Ruleset name: main-merge-queue
Enforcement status: Disabled（配置和静态核对阶段）
Bypass list: 留空
Target branches: Include default branch（main）
```

启用：

- `Require a pull request before merging`，审批数量沿用仓库现有策略；
- 勾选当前页面显示的 `Require status checks to pass`。GitHub 官方文档把同一规则
  称为 `Require status checks to pass before merging`；当前 UI 省略了
  `before merging`，右侧说明中的 `before the ref is updated` 表示相同语义。
  勾选后展开配置区域，使用 `Add checks` 添加下面 5 个精确 job 名，每项的
  `Expected source` 选择 `GitHub Actions`：

  ```text
  trusted-source
  unit-test
  build
  e2e
  jenkins-regression
  ```

- `Require merge queue`，设置：

  ```text
  Merge method: 选择仓库已启用的合并方式（优先沿用当前方式）
  Build concurrency: 1
  Require all queue entries to pass required checks: 开启（API 名称 ALLGREEN）
  Status check timeout: 360 minutes
  ```

`Merge method` 需要手动选择一次。`Pull Requests` 不是左侧菜单：如需核对仓库
现有设置，应进入 `dingodb/dingofs → Settings → General`，在 General 页面内向下
滚动并查找 `Pull Requests` 区块，其中会显示 `Allow merge commits`、
`Allow squash merging` 和 `Allow rebase merging`。这一步只用于查看，不要为了本次
门禁修改仓库原有合并策略。回到 Merge Queue，在 `Merge method` 下拉框中选择团队
当前使用且仓库已允许的方式；如果下拉框只有一种可用方式，直接选择该项。

`Require branches to be up to date before merging` 保持未勾选。它不是 Merge Queue
的替代项，也不需要与 Merge Queue 同时开启：Merge Queue 会自动把待合并 PR 放到
最新 `main` 和队列中前序改动之后，生成新的 merge-group SHA，再对这个 SHA 执行
五个 Required checks。

如果 5 个 check 尚未出现在选择列表中，先把本次 workflow 变更合入 `main`，
运行一次普通 PR，让这些 job 名至少上报一次，再返回 Ruleset 添加。不要用
`PR Check / jenkins-regression`、workflow 文件名或 Jenkins Job 名替代上面的
裸 job 名。

### 13.5 非 Enterprise 的 PR Source 检查

非 Enterprise 组织无法使用原设计中的 Organization Ruleset
`Require workflows to pass before merging`，因此不能把
`.github/workflows/pr-source.yml@refs/heads/main` 作为由 GitHub 固定来源路径的
required workflow。`pr-source.yml` 仍会通过 `pull_request_target` 从 base
repository 的 `main` 运行；`trusted-source` 对同仓和 fork PR 都返回成功，但这不
等价于 Enterprise required-workflow 的强来源绑定。

本节不需要创建额外的 Ruleset、Secret、Environment、reviewer 或 webhook。需要
人工执行的只有以下配置和审查操作：

1. 确认 `.github/workflows/pr-source.yml` 已合入 `main`；
2. 普通 PR 页面出现 `PR Source / trusted-source` 后检查结果：同仓分支和 fork PR
   都应成功；
3. fork PR 完成仓库正常的代码审查和审批后，可以直接加入 Merge Queue；
4. fork PR 不需要由维护者 cherry-pick 到 `dingodb/dingofs` 内的新分支；
5. `jenkins-regression` Environment 保持无 Required reviewers，以便 Merge Queue
   全自动触发并等待 Jenkins Job。

不要把普通 Required status check `trusted-source` 当作完全等价的替代品：
Expected source 只能限定为 `GitHub Actions`，不能固定到具体 workflow path 和 ref。
当前配置明确接受以下信任边界：

1. 同仓和 fork PR 都可以进入 Merge Queue，并自动运行 Jenkins 门禁；
2. `jenkins-regression` 会从受保护 `main` 检出
   `.github/scripts/trigger-jenkins.sh`，但 Environment 中的 Jenkins 凭据仍会自动
   提供给 merge-group job；
3. 审查 fork PR 时必须重点检查其对 `.github/workflows/`、`.github/scripts/` 和
   Jenkins 相关文件的修改；当前方案不增加 Environment 人工审批；
4. 不配置 Organization required-workflow Ruleset，也不要把
   `.github/workflows/pr-check.yml` 作为 required workflow 重复调度。

这是为保持 fork PR 全自动入队而明确接受的安全取舍。如果以后升级 Enterprise，
应优先恢复由 Organization Ruleset 固定 `dingodb/dingofs`、
`.github/workflows/pr-source.yml`、`refs/heads/main` 的方案。

### 13.6 Disabled 配置与 Active 上线

真实 GitHub Actions → Jenkins 链路只能在 workflow、trigger 和 Jenkinsfile
变更进入受保护 `main` 后验证。非 Enterprise 没有 Evaluate mode，按下面的
Disabled 静态核对、Active 成功验收、Active 受控失败验收三个阶段上线。

#### 13.6.1 上线前确认

先完成以下检查，不满足时不要把 Ruleset 切换为 Active：

1. 在 GitHub 打开 `main` 分支，确认以下三个文件已存在且是准备上线的版本：

   ```text
   .github/workflows/pr-check.yml
   .github/workflows/pr-source.yml
   .github/scripts/trigger-jenkins.sh
   ```

2. 进入 `Settings → Secrets and variables → Actions → Variables`，确认：

   ```text
   JENKINS_URL=https://lapping-diagnoses-unbeaten.ngrok-free.dev
   JENKINS_JOB_PATH=dingofs-workflow-PR-127
   ```

3. 进入 `Settings → Environments → jenkins-regression`，确认没有 Required
   reviewers，branch pattern 为 `gh-readonly-queue/main/*`，并且 Environment
   secrets 中存在 `JENKINS_USER` 和 `JENKINS_API_TOKEN`。GitHub 不会显示 secret
   原值，只需确认名称存在且最后更新时间正确。
4. 在 Jenkins 打开 `Manage Jenkins → Nodes`，选择实际绑定
   `172.30.14.127` 的节点，确认状态为 Online、label 包含
   `dingofs-regression`，并且至少有一个空闲 executor。
5. 打开 `Manage Jenkins → Lockable Resources`，确认
   `dingofs-regression-env` 存在且状态为 Free。
6. 打开 Job `dingofs-workflow-PR-127 → Configure`，确认使用受保护的
   `Pipeline script`、参数名与本文件第 6 节一致，并已保存最新 Jenkinsfile。
7. 按 4.3 和第 11 节完成 agent 与 Jenkins API 预检；确认 ngrok URL 可访问，
   Job GET 成功，`buildWithParameters` 返回 2xx 和 queue Location。
8. 在 127 上确认 `regression-unhealthy` 不存在，并记录 Jenkins Job 当前最后一个
   build number，后续用它判断普通 PR 是否误触发 Jenkins：

   ```bash
   ssh jenkins@172.30.14.127 \
     'test ! -e /home/jenkins/.cache/dingofs-ci/regression-unhealthy'
   ```

#### 13.6.2 以 Disabled 保存并静态核对

1. 进入 `dingodb/dingofs → Settings → Rulesets → Rulesets`。
2. 打开 `main-merge-queue`；如果尚未创建，按 13.4 创建。
3. 将 `Enforcement status` 设为 `Disabled`。
4. 逐项核对并保存：

   ```text
   Target branches: Include default branch（main）
   Bypass list: 空
   Require a pull request before merging: 开启
   Require status checks to pass: 开启
   Required checks: trusted-source、unit-test、build、e2e、jenkins-regression
   Expected source: 五项均为 GitHub Actions
   Require branches to be up to date before merging: 关闭
   Require merge queue: 开启
   Merge method: 仓库当前允许并正在使用的方式
   Build concurrency: 1
   Require all queue entries to pass required checks: 开启
   Status check timeout: 360 minutes
   ```

5. 返回 Rulesets 列表，确认 `main-merge-queue` 显示 `Disabled`。此时规则既不
   执行也不阻止合并，不能把 Disabled 阶段当作功能验证。
6. 检查是否还有其他 Repository Ruleset 或经典 Branch protection rule 同时匹配
   `main`。GitHub 会叠加全部规则；如存在，记录其 required checks、审批数和
   merge method，先确认没有冲突，不要直接删除现有规则。

#### 13.6.3 切换 Active

安排一个暂停日常合并的验收窗口，然后执行：

1. 进入 `Settings → Rulesets → Rulesets → main-merge-queue`。
2. 把 `Enforcement status` 从 `Disabled` 改为 `Active`，点击 `Save changes`。
3. 返回 Rulesets 列表，确认状态显示 `Active`。
4. 打开一个已有的普通 PR 或测试 PR，确认合并区出现 Merge Queue 相关要求，且
   管理员没有选择 `Merge without waiting for requirements to be met` 绕过规则。

#### 13.6.4 验证普通 PR 不触发 Jenkins

1. 从个人 fork 创建测试分支，提交一个可安全合入的小型文档改动，并创建目标为
   `dingodb/dingofs:main` 的 PR。
2. 打开 PR 的 `Checks` 页面，等待 `PR Check` 完成。预期结果为：

   ```text
   unit-test: success
   build: skipped
   e2e: skipped
   jenkins-regression: skipped
   PR Source / trusted-source: success
   ```

3. 打开 Jenkins Job 的 `Build History`，确认最后一个 build number 没有因为这个
   普通 PR 增加。若增加，立即按 13.6.7 回退，不要把 PR 加入队列。
4. 按仓库正常规则完成代码审查和审批。

#### 13.6.5 验证成功的 Merge Queue 链路

1. 在测试 PR 合并区点击 `Merge when ready`，再点击
   `Confirm merge when ready`。不要选择管理员 bypass。
2. 进入仓库 `Actions → PR Check`，打开新产生的 run。确认该 run 来自
   `merge_group`，其 ref 以
   `refs/heads/gh-readonly-queue/main/` 开头，而不是 PR head branch。
3. 观察 job 顺序，必须依次为：

   ```text
   unit-test → build → e2e → jenkins-regression
   ```

   后一个 job 在前一个成功前不应开始；`jenkins-regression` 之前不应产生新的
   Jenkins build。
4. 打开 `jenkins-regression → Trigger Jenkins regression and wait` 日志，找到：

   ```text
   Jenkins build: https://.../job/dingofs-workflow-PR-127/<BUILD_NUMBER>/
   ```

5. 打开该 Jenkins URL，在 `Parameters` 或 Console Output 中核对：

   ```text
   GIT_REF = 当前 gh-readonly-queue/main/* ref
   GIT_SHA = 当前 merge_group SHA
   GITHUB_RUN_ID = 当前 GitHub Actions run ID
   GITHUB_REPOSITORY = dingodb/dingofs
   GITHUB_SERVER_URL = https://github.com
   ```

6. Jenkins 应依次完成 checkout、build、创建独立集群、运行
   `dtt smoke --env env_127`、收集证据和销毁集群。最终 Jenkins result 必须是
   `SUCCESS`，GitHub 日志必须出现：

   ```text
   jenkins-trigger: Jenkins finished with SUCCESS
   ```

7. 回到 GitHub，确认 `trusted-source`、`unit-test`、`build`、`e2e`、
   `jenkins-regression` 五项均为 success，测试 PR 随后由 Merge Queue 合入
   `main`。
8. 在 Jenkins 的该 build 页面确认 `jenkins-regression-artifacts` 已归档；在 127
   上确认集群与 guard 已清理、固定端口没有监听：

   ```bash
   ssh jenkins@172.30.14.127 '
     set -e
     test ! -e /home/jenkins/.cache/dingofs-ci/regression-unhealthy
     ! /home/jenkins/.dingo/bin/dingo cluster ls | awk '\''$1 == "main-dingofs" {found=1} END {exit !found}'\''
     for port in 6500 6501 6502 7500 7501 7502 \
                 6600 6601 6602 7600 7601 7602 \
                 6900 6901 6902 18765 13307; do
       test -z "$(ss -H -ltn "sport = :${port}")"
     done
   '
   ```

9. 用 `GITHUB_RUN_ID` 检查 Jenkins Build History，确认这个 merge-group 只有一个
   对应 build；若同一 run ID 出现两次，说明存在重复调度，不能继续上线。

#### 13.6.6 验证 Jenkins 非 SUCCESS 会阻止合并

不要通过破坏 Secret、修改 127 环境或提交故意编译失败的代码制造失败。推荐使用
Jenkins `ABORTED` 做安全的受控失败；触发脚本对 `FAILURE`、`UNSTABLE`、
`ABORTED` 和未知结果均采用相同的失败路径。

1. 再创建一个 fork 文档测试 PR，完成普通 PR 检查与审批，然后点击
   `Merge when ready` 加入 Merge Queue。
2. 等待 `unit-test`、`build`、`e2e` 成功，并在 `jenkins-regression` 日志中取得
  新的 Jenkins build URL。
3. 立即打开 Jenkins build，优先在进入 `Deploy candidate binaries` 之前点击
   `Stop/Abort` 并确认。这样可以验证结果传播，同时尽量避免改动共享环境。
4. 等待 Jenkins 显示 `ABORTED`。GitHub Actions 日志应出现：

   ```text
   jenkins-trigger: Jenkins finished with ABORTED
   ```

5. 回到 GitHub，确认：

   ```text
   unit-test: success
   build: success
   e2e: success
   jenkins-regression: failure
   PR: 未合并，并被 Merge Queue 移除
   main: 不包含该测试 PR 的提交
   ```

6. 关闭这个失败测试 PR，或保留用于修复后重新验收；不要管理员绕过失败 check。
7. 如果 Abort 时 Pipeline 已进入 `Deploy candidate binaries`，必须等 Jenkins 的
   `finally` 清理结束，再按 13.6.5 第 8 步检查 guard、集群和端口。guard 存在时
   禁止直接重跑，也不要盲目删除 marker，按 `.github/CICD.md` 手工恢复。

#### 13.6.7 异常回退

任一步骤不符合预期时立即执行：

1. 暂停所有对 `main` 的人工合并和新的 Merge Queue 入队操作。
2. 进入 `Settings → Rulesets → Rulesets → main-merge-queue`，把
   `Enforcement status` 从 `Active` 改回 `Disabled` 并保存。
3. 从 PR 的 Merge Queue 页面移除尚未执行的测试 PR；对正在运行的 GitHub
   Actions run 点击 `Cancel workflow`。
4. GitHub run 被取消后，触发脚本会尝试取消 Jenkins queue item 或停止 Jenkins
   build。仍需打开 Jenkins Build History，人工确认没有该 run 遗留的 queued 或
   running build。
5. 在 127 检查 `regression-unhealthy`、`main-dingofs` 集群和固定端口。任何一项
   未清理都按 `.github/CICD.md` 恢复，在环境恢复前不得重跑。
6. 记录失败的 GitHub run URL、Jenkins build URL、`GITHUB_RUN_ID`、`GIT_SHA` 和
   归档证据。修复后从 13.6.1 重新开始，成功和受控失败场景必须全部重验。

Ruleset 处于 Disabled 时不会保护 `main`，因此回退后必须依靠“暂停人工合并”避免
绕过门禁；不能依赖 Rule Insights，因为非 Enterprise 没有 Evaluate mode。

## 14. 常见错误

| 现象 | 原因与处理 |
|---|---|
| 一直显示 `Waiting for next available executor` | node 不在线，或缺少 `dingofs-regression` label |
| `No such DSL method 'timestamps'` | 缺少 Timestamper 插件 |
| `No such DSL method 'lock'` | 缺少 Lockable Resources 插件 |
| 出现 Script Approval | 没有勾选 Groovy Sandbox，或插件不完整 |
| API 返回 HTTP 403 | token、`Overall/Read` 或 Job 级权限不正确；Jenkins 常直接返回 403 而不是先返回 401 |
| API 返回 HTTP 404 | Job 名或 `JENKINS_JOB_PATH` 不正确 |
| 空参数构建报 GIT_SHA 错误 | 这是第一次无副作用验证的预期结果 |
| 日志显示 `Shared environment is quarantined` | 持久 guard 已存在，禁止重跑；先按 `.github/CICD.md` 的手工清理步骤处理 |
| queue item 长时间不启动 | 检查 node executor、`disableConcurrentBuilds()` 和 `dingofs-regression-env` 状态 |
| merge-group ref fetch 失败 | 确认临时 ref 仍存在；禁止回退到 `main` |
| `dtt smoke --env env_127` 失败 | 在 127 本机维护 DTT 与 `env_127`；Pipeline 不会下载、构建或修改 DTT |
| cluster deploy/status 失败 | 查看归档 topology、cluster status 和 container logs；不要单独删除 guard |

## 15. 完成检查表

### 15.1 需要人工完成的操作

下面这些外部状态无法由仓库文件自动创建，需要你或相应管理员手工完成：

| 操作位置 | 人工操作 |
|---|---|
| Jenkins Controller | 安装/确认插件，创建 Lockable Resource、服务账号、API Token 和 Job 级权限 |
| Jenkins Agent 127 | 配置 label，确认本机 `dingo`、`dtt`、`env_127` 和依赖可用 |
| Jenkins Job | 把本分支最新 Jenkinsfile 复制到受保护的 `Pipeline script` 字段 |
| Git 仓库 | review 后 commit/push，并通过现有流程把 workflow、trigger、文档变更合入 `main` |
| GitHub Repository | 创建 Variables、Environment、Environment secrets 和仓库级 main Ruleset |
| PR 来源管理 | fork PR 可直接入队；重点审查 workflow、trigger 和 Jenkins 相关文件变更 |
| 上线验收 | Ruleset 先 Disabled，准备完成后切换 Active，运行成功和受控失败的真实 merge-group |

我不会自动执行上述外部写操作。特别是复制 Jenkinsfile、创建或修改 Ruleset、
保存 Secret、commit、push、触发真实 Job 和切换 `Active`，都需要你的明确授权或
由你在 UI 中完成。

### 15.2 最终核对

```text
[ ] Jenkins URL 配置为公网 ngrok 地址
[ ] Pipeline、Lockable Resources、Matrix Authorization、Timestamper 已安装
[ ] Agent Online，label 包含 dingofs-regression
[ ] `env_127` 使用的 MinIO bucket 已预先创建，并已安排人工定时清理
[ ] Agent 只读预检通过
[ ] `/home/jenkins/.dingo/bin/dingo cluster ls` 可执行
[ ] 127 本机 `/home/jenkins/.local/bin/dtt` 与 `env_127` 已由 DTT owner 配置
[ ] dingofs-regression-env 已创建且状态为 Free
[ ] dingofs 已创建
[ ] 管理员 Overall/Administer 保留，未发生权限锁死
[ ] 服务账号全局只有 Overall/Read
[ ] authenticated/所属组没有额外全局 Job 权限
[ ] Job 名为 dingofs-workflow-PR-127
[ ] Job 使用 Pipeline script，不使用 Pipeline script from SCM
[ ] Groovy Sandbox 已勾选
[ ] 服务账号只有 Job/Read、Job/Build、Job/Cancel
[ ] 第一次空参数构建在 Validate request 按预期失败
[ ] 第二次安全参数验证未进入 Build exact SHA
[ ] API whoAmI 和 Job GET 验证成功
[ ] API buildWithParameters 返回 2xx 和 queue Location
[ ] regression-unhealthy guard 不存在
[ ] workflow、trigger 和 Jenkinsfile 相关变更已 review 并合入 main
[ ] Repository Variables 已创建
[ ] jenkins-regression Environment、branch pattern 与两个 secrets 已创建
[ ] main Repository Ruleset 已包含 5 个 Required checks，Expected source 均为 GitHub Actions
[ ] main Repository Ruleset 已启用 Merge Queue：ALLGREEN、concurrency=1、timeout=360
[ ] fork PR 的 trusted-source 成功，并可在正常审批后直接进入 Merge Queue
[ ] Disabled 状态下已完成 Ruleset 静态核对
[ ] Repository Ruleset 已切换为 Active
[ ] Active 下普通 PR 未触发 Jenkins
[ ] Active 下真实 merge-group 成功场景通过
[ ] Active 下 Jenkins 受控失败使 PR 无法合并
```

## 16. 官方参考

- [Jenkins Pipeline Syntax](https://www.jenkins.io/doc/book/pipeline/syntax/)
- [Jenkins Plugin Management](https://www.jenkins.io/doc/book/managing/plugins/)
- [Lockable Resources](https://plugins.jenkins.io/lockable-resources/)
- [Matrix Authorization Strategy](https://plugins.jenkins.io/matrix-auth/)
- [Timestamper](https://plugins.jenkins.io/timestamper/)
- [Authenticating scripted clients](https://www.jenkins.io/doc/book/system-administration/authenticating-scripted-clients/)
- [CSRF Protection](https://www.jenkins.io/doc/book/security/csrf-protection/)
- [GitHub：Creating rulesets for a repository](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/creating-rulesets-for-a-repository)
- [GitHub：Available rules for rulesets](https://docs.github.com/en/enterprise-cloud@latest/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
- [GitHub：Managing a merge queue](https://docs.github.com/en/enterprise-cloud@latest/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue)
- [GitHub：Deployment environments](https://docs.github.com/en/actions/concepts/workflows-and-actions/deployment-environments)
