#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DOC="${ROOT}/.github/JENKINS_JOB_SETUP.md"

require() {
  if ! grep -Eq -- "$1" "${DOC}"; then
    echo "missing Jenkins setup guidance: $1" >&2
    exit 1
  fi
}

forbid() {
  if grep -Eq -- "$1" "${DOC}"; then
    echo "stale Jenkins setup guidance: $1" >&2
    exit 1
  fi
}

require '^### 13\.3 普通 PR 与 Merge Queue 触发语义$'
require '^### 13\.4 Repository Ruleset：保护 main$'
require '^### 13\.5 非 Enterprise 的 PR Source 门禁$'
require '^### 13\.6 Disabled 配置与 Active 上线$'
require '^#### 13\.6\.1 上线前确认$'
require '^#### 13\.6\.2 以 Disabled 保存并静态核对$'
require '^#### 13\.6\.3 切换 Active$'
require '^#### 13\.6\.4 验证普通 PR 不触发 Jenkins$'
require '^#### 13\.6\.5 验证成功的 Merge Queue 链路$'
require '^#### 13\.6\.6 验证 Jenkins 非 SUCCESS 会阻止合并$'
require '^#### 13\.6\.7 异常回退$'
require '普通 PR 不触发 Jenkins'
require 'dingodb.*不是 GitHub Enterprise'
require 'merge_group'
require 'unit-test'
require 'build'
require 'e2e'
require 'jenkins-regression'
require 'trusted-source'
require 'Expected source.*GitHub Actions'
require 'Require merge queue'
require 'ALLGREEN'
require 'Build concurrency.*1'
require 'Status check timeout.*360'
require 'dingodb/dingofs'
require '\.github/workflows/pr-source\.yml'
require 'refs/heads/main'
require 'Settings → Rulesets → Rulesets'
require 'Enforcement status: Disabled'
require 'Evaluate mode.*不可用'
require 'Active'
require '出现问题.*Disabled'
require 'Jenkins.*SUCCESS'
require 'Jenkins.*FAILURE'
require 'jenkins-trigger: Jenkins finished with SUCCESS'
require 'jenkins-trigger: Jenkins finished with ABORTED'
require 'Merge without waiting for requirements to be met'
require '^### 15\.1 需要人工完成的操作$'
require 'Pipeline 不会创建、清空或验证 MinIO bucket'
require '人工定时清理'

forbid '\[ \] 两个固定 node 环境变量已配置'
forbid '\[ \] `/home/jenkins/\.dingo/bin/dingo` 可执行，127 本机免密 SSH 通过'
forbid 'DINGOFS_REGRESSION_MINIO_ENDPOINT'
forbid 'mc alias export'
forbid 'Enforcement status: Evaluate'
forbid 'Organization Ruleset 已固定'
forbid '两套 Ruleset'

echo 'PASS: Jenkins setup documentation contract'
