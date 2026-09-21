#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
TEMP_ROOT=$(mktemp -d)
trap 'rm -rf -- "${TEMP_ROOT}"' EXIT

export PATH="${ROOT}/scripts/test/fixtures/jenkins:${PATH}"
export FAKE_CURL_LOG="${TEMP_ROOT}/curl.log"
export JENKINS_URL=http://jenkins.test
export JENKINS_JOB_PATH=dingofs-regression
export JENKINS_USER=dingofs
export JENKINS_API_TOKEN=test-token
export PR_NUMBER=1083
export PR_AUTHOR=octocat
export GIT_REF=refs/heads/gh-readonly-queue/main/pr-1083-deadbeef
export GIT_SHA=0123456789abcdef0123456789abcdef01234567
export GITHUB_RUN_ID=123456
export GITHUB_REPOSITORY=dingodb/dingofs
export GITHUB_SERVER_URL=https://github.com
export JENKINS_POLL_INTERVAL_SECONDS=0
export JENKINS_WAIT_TIMEOUT_SECONDS=10

bash "${ROOT}/.github/scripts/trigger-jenkins.sh" \
  >"${TEMP_ROOT}/success.stdout" 2>"${TEMP_ROOT}/success.stderr"

grep -Fx 'PR_NUMBER=1083' "${FAKE_CURL_LOG}"
grep -Fx 'PR_AUTHOR=octocat' "${FAKE_CURL_LOG}"
grep -F 'Jenkins finished with SUCCESS' "${TEMP_ROOT}/success.stdout"

if env -u PR_AUTHOR bash "${ROOT}/.github/scripts/trigger-jenkins.sh" \
    >"${TEMP_ROOT}/missing.stdout" 2>"${TEMP_ROOT}/missing.stderr"; then
  echo 'trigger unexpectedly accepted a missing PR_AUTHOR' >&2
  exit 1
fi
grep -F 'jenkins-trigger: missing PR_AUTHOR' "${TEMP_ROOT}/missing.stderr"

echo 'PASS: Jenkins PR metadata trigger contract'
