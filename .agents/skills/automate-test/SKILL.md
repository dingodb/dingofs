---
name: automate-test
description: 开发环境测试闭环：单元测试 / e2e / 重型回归 / xfstests。
context: fork
disable-model-invocation: true
---

# dingofs 自动化测试技能

自动化测试闭环：跑测试 → 定位失败 → 修代码 → 重编译重部署 → 再跑。仅限开发环境，不用于生产，不自行 git 提交。

## 前置：服务就绪

编译、MDS/client 的部署启动、日志位置全部见 `/skill:dev-deploy`。本技能假设 dist/ 下服务已在跑，**不重复部署步骤**。

## 编译

```bash
cd build && make -j 12
```

单元测试二进制要求 build 以 `-DBUILD_UNIT_TESTS=ON` 配置（当前 build/ 已满足）。若 `build/bin/test` 不存在或为空，重新配置：

```bash
cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DBUILD_UNIT_TESTS=ON .. && make -j 12
```

## 单元测试

`build/bin/test` 下是 gtest 二进制，逐个直接运行，无需参数。`test_coverage_helper` 是覆盖率辅助程序，跳过。

判据：进程退出码为 0，且输出中无 `[  FAILED  ]`。

## 端到端与重型工具

`run_all_test.sh` 封装了 e2e / pjdfstest / fsx / mdtest / fio / fsstress / vdbench / xfstests。**除 `xfstests` 外 `--mountpoint` 必填**，不传直接退出。

```bash
cd scripts/dev-mds
bash run_all_test.sh --mountpoint=$MOUNT_POINT --type=e2e --round=1
```

| 场景 | `--type` |
|---|---|
| 日常回归 | `e2e` |
| 全量重型（e2e / pjdfstest / fsx / mdtest / fsstress / vdbench） | `all` |
| 单个工具 | `pjdtest` \| `fsx` \| `mdtest` \| `fio` \| `fsstress` \| `vdbench` \| `xfstests` |

- `--round` 默认 1；只有反复跑找偶发才需要调大。
- e2e 依赖 `test/e2e` 的 uv 环境；pjdfstest 依赖 `/home/dengzihui/work/dingofs-test/pjdfstest/tests` 存在。
- `--mds-addr` 在脚本里已定义但未使用，不要传。
- `xfstests` 自拉两套挂载、不用 `--mountpoint`，且**不进 `all`**，需显式指定。

判据：退出码 0，且输出中没有 `result: FAIL` —— **脚本只对 e2e / pjdfstest / fsx / vdbench / xfstests 判定**；mdtest / fio / fsstress 不判定，需自己查日志确认无 error。

日志：`/tmp/dev-regression-test/<tool>_<时间戳>_<轮次>/`。

## xfstests

适配层的安装、产物、local/MDS 两种模式见仓库 `xfstests/README_CN.md`，不在此重复。

```bash
# 一次性：安装挂载 helper 并生成配置；meta-url 按需改
DINGOFS_META_URL_TEMPLATE="mds://<SERVER_HOST>:7801/{fsname}" \
  bash xfstests/setup.sh /home/dengzihui/work/dingofs-test/xfstests-dev
```

**推荐用封装脚本**（跑前自动 `reset.sh`，`results/` 收进日志目录）：

```bash
cd scripts/dev-mds
bash run_all_test.sh --type=xfstests --round=1
```

它按仓库 `xfstests/supported` 清单跑；`--mountpoint` 对本 type 无意义、无需传。
预检缺件时只报错不自动装，按提示跑 `setup.sh`。

手动跑：

```bash
cd /home/dengzihui/work/dingofs-test/xfstests-dev
sudo ./check $(grep -vE '^[[:space:]]*(#|$)' <仓库根>/xfstests/supported)
```

`<SERVER_HOST>` 从 `scripts/dev-mds/mds_deploy_parameters.local` 取。

判据：`Passed all <N> tests` 且 N>0，且无 `Failures:`。
失败证据：日志目录下 `results/generic/NNN.out.bad`（测试侧）、`/mnt/dingofs-xfstests/runtime/<fsname>/log/`（client 侧）。

## vdbench

**推荐用封装脚本**（已进 `all`，只跑它时显式指定）：

```bash
cd scripts/dev-mds
bash run_all_test.sh --mountpoint=$MOUNT_POINT --type=vdbench --round=1
```

参数文件由脚本按 `--mountpoint` 内联生成：`validate=yes`、`data_errors=1`、o_direct、随机混合读写、`elapsed=300`，锚定 `$MOUNT_POINT/vdbench_test_<时间戳>_<轮次>`。

判据：脚本判定 —— 退出码 0 且无 Data Validation / I/O 错误。
日志 `vdbench.log`，报告在 `output/`（`errorlog.html`、`summary.html`、`flatfile.html`）。
预检缺 vdbench 或 java 只报 FAIL、不自动装（vdbench 在 `/home/dengzihui/work/dingofs-test/vdbench`）。

手动跑（改 `config/test-01.vd` 的 `anchor=` 指向 `$MOUNT_POINT` 下、`elapsed` 改成回归可接受秒数，首次 `format=yes`）：

```bash
cd /home/dengzihui/work/dingofs-test/vdbench
./vdbench -f config/test-01.vd
```

## 测试对象地址

第 1 个 MDS 实例监听 `<SERVER_HOST>:<SERVER_START_PORT + 1>`（默认 7801），取值见 `scripts/dev-mds/mds_deploy_parameters.local`。

- meta: `mds://<SERVER_HOST>:7801/<fs_name>`
- fs 不存在时先创建（需要 MDS 已在跑）：`cd scripts/dev-mds && bash create_fs.sh --fs_name=$FS_NAME --mds_addr=<SERVER_HOST>:7801`

## 流程

1. **编译**：`cd build && make -j 12` 成功。
2. **确认服务在跑**：`pgrep -c -x dingo-mds` 等于 `SERVER_NUM`，且 `mountpoint -q $MOUNT_POINT` 成立；不一致先按 `/skill:dev-deploy` 重部署。（不要用 `ps -ef | grep`：会匹配到自己，且本机可能另有部署的 client。）
3. **执行测试**：单元测试逐个跑，或 `run_all_test.sh --type=...`；需要时加 xfstests / vdbench。记录到 trace（见下）。
4. **判定**：按各自判据全绿 → 跳 6；有失败 → 下一步。
5. **定位并修复**：e2e 失败看对应 `result` 上方的日志目录，单元测试看 stderr，结合 `dist/*/log/` 里的服务日志定位根因，改代码后回到 1。
   **同一个测试连续 3 轮仍不通过就停手**，把已定位的根因、试过的改法、日志路径汇报给用户，不要继续盲改。
6. **报告**：给出变更清单与测试结论。**不要自行 git 提交**，提交交给用户或 `/skill:git-commit`。

## 跟踪

每完成上面一步，向 `/tmp/automate-test.trace` 追加一行：

```
<时间> <步骤号> <命令> <结果> <日志路径>
```
