---
name: automate-test
description: 在开发环境对dingofs进行自动化测试
context: fork
disable-model-invocation: true
---

# dingofs自动化测试技能

自动化测试闭环：跑测试 → 定位失败 → 修代码 → 重编译重部署 → 再跑。覆盖单元测试与端到端测试。

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

## 端到端测试

`run_all_test.sh` 封装了 e2e / pjdfstest / fsx / mdtest / fio / fsstress。**`--mountpoint` 必填**，不传直接退出。

```bash
cd scripts/dev-mds
bash run_all_test.sh --mountpoint=$MOUNT_POINT --type=e2e --round=1
```

- `--type`: `all`(默认) | `e2e` | `pjdtest` | `fsx` | `mdtest` | `fio` | `fsstress`。日常回归用 `e2e`；`all` 会连带 fsx(默认 1 小时)、fsstress(10000 ops)，只在需要时用。
- e2e 依赖 `test/e2e` 的 uv 环境；pjdfstest 依赖 `/home/dengzihui/work/dingofs-test/pjdfstest/tests` 存在。
- `--mds-addr` 在脚本里已定义但未使用，不要传。

判据：每个工具打印的 `### [x] result: PASS` 全部为 PASS（无 FAIL），且脚本退出码为 0。日志在 `/tmp/dev-regression-test/<tool>_<时间戳>_<轮次>/`。

## 测试对象地址

第 1 个 MDS 实例监听 `<SERVER_HOST>:<SERVER_START_PORT + 1>`（默认 7801），取值见 `scripts/dev-mds/mds_deploy_parameters.local`。

- meta: `mds://<SERVER_HOST>:7801/<fs_name>`
- fs 不存在时先创建（需要 MDS 已在跑）：`cd scripts/dev-mds && bash create_fs.sh --fs_name=$FS_NAME --mds_addr=<SERVER_HOST>:7801`

## 流程

1. **编译**：`cd build && make -j 12` 成功。
2. **确认服务在跑**：`ps -ef | grep -E 'dingo-mds|dingo-client'` 进程数与 `SERVER_NUM` 一致；不一致先按 `/skill:dev-deploy` 重部署。
3. **执行测试**：单元测试逐个跑，或 `run_all_test.sh --type=...`。记录到 trace（见下）。
4. **判定**：全绿 → 跳 6；有失败 → 下一步。
5. **定位并修复**：e2e 失败看对应 `result` 上方的日志目录，单元测试看 stderr，结合 `dist/*/log/` 里的服务日志定位根因，改代码后回到 1。
   **同一个测试连续 3 轮仍不通过就停手**，把已定位的根因、试过的改法、日志路径汇报给用户，不要继续盲改。
6. **报告**：给出变更清单与测试结论。**不要自行 git 提交**，提交交给用户或 `/skill:git-commit`。

## 跟踪

每完成上面一步，向 `/tmp/automate-test.trace` 追加一行：

```
<时间> <步骤号> <命令> <结果> <日志路径>
```
