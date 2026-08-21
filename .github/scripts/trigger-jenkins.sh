#!/usr/bin/env bash
set -euo pipefail

required=(
  JENKINS_URL
  JENKINS_JOB_PATH
  JENKINS_USER
  JENKINS_API_TOKEN
  GIT_REF
  GIT_SHA
  GITHUB_RUN_ID
  GITHUB_REPOSITORY
  GITHUB_SERVER_URL
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "jenkins-trigger: missing ${name}" >&2
    exit 2
  fi
done

if [[ ! "${GIT_SHA}" =~ ^[0-9a-fA-F]{40}$ ]]; then
  echo "jenkins-trigger: GIT_SHA must be a full commit SHA" >&2
  exit 2
fi
if [[ "${GIT_REF}" != refs/heads/gh-readonly-queue/main/* ]]; then
  echo "jenkins-trigger: GIT_REF is not a main merge-queue ref" >&2
  exit 2
fi
command -v curl >/dev/null || {
  echo "jenkins-trigger: curl is required" >&2
  exit 2
}
command -v jq >/dev/null || {
  echo "jenkins-trigger: jq is required" >&2
  exit 2
}

base=${JENKINS_URL%/}
poll_interval=${JENKINS_POLL_INTERVAL_SECONDS:-15}
wait_timeout=${JENKINS_WAIT_TIMEOUT_SECONDS:-15600}
if [[ ! "${poll_interval}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "jenkins-trigger: poll interval must be numeric" >&2
  exit 2
fi
if [[ ! "${wait_timeout}" =~ ^[0-9]+$ ]]; then
  echo "jenkins-trigger: wait timeout must be an integer" >&2
  exit 2
fi

queue_id=""
build_number=""
remote_complete=0
temp_dir=$(mktemp -d)

urlencode() {
  jq -nr --arg value "$1" '$value | @uri'
}

job_url=${base}
IFS='/' read -r -a job_parts <<<"${JENKINS_JOB_PATH}"
for part in "${job_parts[@]}"; do
  if [[ -z "${part}" ]]; then
    echo "jenkins-trigger: invalid job path" >&2
    exit 2
  fi
  job_url+="/job/$(urlencode "${part}")"
done

auth_curl() {
  curl --silent --show-error --connect-timeout 10 --max-time 30 \
    --user "${JENKINS_USER}:${JENKINS_API_TOKEN}" "$@"
}

cleanup_curl() {
  auth_curl --connect-timeout 2 --max-time 5 "$@"
}

queue_id_from_headers() {
  local header_file=$1
  local location=""
  [[ -s "${header_file}" ]] || return 1
  location=$(awk 'tolower($1) == "location:" {
    $1 = ""; sub(/^[[:space:]]*/, ""); sub(/\r$/, ""); print; exit
  }' "${header_file}")
  [[ "${location}" =~ /queue/item/([0-9]+)/?$ ]] || return 1
  printf '%s\n' "${BASH_REMATCH[1]}"
}

cancel_remote() {
  local cleanup_build_number=${build_number}
  local recovered_queue_id=""
  local queue_json=""
  local discovered_build=""
  ((remote_complete == 0)) || return 0
  if [[ "${cleanup_build_number}" =~ ^[0-9]+$ ]]; then
    if ! cleanup_curl --fail --request POST --output /dev/null \
      "${job_url}/${cleanup_build_number}/stop"; then
      echo "jenkins-trigger: failed to stop Jenkins build ${cleanup_build_number}" >&2
    fi
    cleanup_build_number=""
  fi
  if [[ -z "${queue_id}" ]] &&
      recovered_queue_id=$(queue_id_from_headers "${headers:-}"); then
    queue_id=${recovered_queue_id}
  fi
  if [[ -n "${queue_id}" ]]; then
    if ! cleanup_curl --fail --request POST --output /dev/null \
      "${base}/queue/cancelItem?id=${queue_id}"; then
      echo "jenkins-trigger: failed to cancel Jenkins queue item ${queue_id}" >&2
    fi
    if queue_json=$(cleanup_curl --fail \
        "${base}/queue/item/${queue_id}/api/json"); then
      if discovered_build=$(jq -r '.executable.number // empty' \
          <<<"${queue_json}"); then
        if [[ -n "${discovered_build}" ]]; then
          cleanup_build_number=${discovered_build}
        fi
      else
        echo "jenkins-trigger: failed to parse Jenkins queue item ${queue_id}" >&2
      fi
    else
      echo "jenkins-trigger: failed to inspect Jenkins queue item ${queue_id}" >&2
    fi
  fi
  if [[ "${cleanup_build_number}" =~ ^[0-9]+$ ]]; then
    if ! cleanup_curl --fail --request POST --output /dev/null \
      "${job_url}/${cleanup_build_number}/stop"; then
      echo "jenkins-trigger: failed to stop Jenkins build ${cleanup_build_number}" >&2
    fi
  fi
}

finish() {
  local rc=$?
  trap - EXIT HUP INT TERM
  if ((rc != 0)); then
    cancel_remote
  fi
  rm -rf "${temp_dir}"
  exit "${rc}"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

headers=${temp_dir}/headers
body=${temp_dir}/body
status=$(auth_curl --request POST --dump-header "${headers}" --output "${body}" \
  --write-out '%{http_code}' \
  --data-urlencode "GIT_REF=${GIT_REF}" \
  --data-urlencode "GIT_SHA=${GIT_SHA}" \
  --data-urlencode "GITHUB_RUN_ID=${GITHUB_RUN_ID}" \
  --data-urlencode "GITHUB_REPOSITORY=${GITHUB_REPOSITORY}" \
  --data-urlencode "GITHUB_SERVER_URL=${GITHUB_SERVER_URL}" \
  "${job_url}/buildWithParameters")
if [[ ! "${status}" =~ ^2[0-9][0-9]$ ]]; then
  echo "jenkins-trigger: trigger returned HTTP ${status}" >&2
  exit 1
fi

if ! queue_id=$(queue_id_from_headers "${headers}"); then
  echo "jenkins-trigger: missing valid queue Location" >&2
  exit 1
fi
deadline=$((SECONDS + wait_timeout))

while ((SECONDS <= deadline)); do
  queue_json=$(auth_curl --fail "${base}/queue/item/${queue_id}/api/json")
  if [[ $(jq -r '.cancelled // false' <<<"${queue_json}") == true ]]; then
    echo "jenkins-trigger: Jenkins queue item was cancelled" >&2
    exit 1
  fi
  build_number=$(jq -r '.executable.number // empty' <<<"${queue_json}")
  [[ -z "${build_number}" ]] || break
  sleep "${poll_interval}"
done
if [[ ! "${build_number}" =~ ^[0-9]+$ ]]; then
  echo "jenkins-trigger: timed out waiting for a build number" >&2
  exit 1
fi

build_url="${job_url}/${build_number}"
echo "Jenkins build: ${build_url}/"
while ((SECONDS <= deadline)); do
  build_json=$(auth_curl --fail "${build_url}/api/json")
  if [[ $(jq -r '.building' <<<"${build_json}") == false ]]; then
    result=$(jq -r '.result // "UNKNOWN"' <<<"${build_json}")
    remote_complete=1
    if [[ "${result}" != SUCCESS ]]; then
      echo "jenkins-trigger: Jenkins finished with ${result}" >&2
      exit 1
    fi
    echo "jenkins-trigger: Jenkins finished with SUCCESS"
    exit 0
  fi
  sleep "${poll_interval}"
done

echo "jenkins-trigger: timed out waiting for Jenkins completion" >&2
exit 1
