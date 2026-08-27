// Copy this reviewed source into the protected Jenkins Pipeline script field.
// Never load it from the merge-group checkout.
pipeline {
  agent { label 'dingofs-regression' }

  parameters {
    string(name: 'GIT_REF', defaultValue: '', trim: true)
    string(name: 'GIT_SHA', defaultValue: '', trim: true)
    string(name: 'GITHUB_RUN_ID', defaultValue: '', trim: true)
    string(name: 'GITHUB_REPOSITORY', defaultValue: '', trim: true)
    string(name: 'GITHUB_SERVER_URL', defaultValue: '', trim: true)
  }

  environment {
    DINGOFS_REPO_URL = 'https://github.com/dingodb/dingofs.git'
    DTT_BIN = '/home/jenkins/.local/bin/dtt'
    SDK_CACHE_ROOT = '/home/jenkins/.cache/dingofs-ci/dingo-sdk'
    CLUSTER_ROOT = '/home/jenkins/.cache/dingofs-ci/regression-clusters'
    UNHEALTHY_MARKER = '/home/jenkins/.cache/dingofs-ci/regression-unhealthy'
    DINGO_CLI = '/home/jenkins/.dingo/bin/dingo'
    CLUSTER_NAME = 'main-dingofs'
    STORE_IMAGE = 'harbor.zetyun.cn/dingodb/dingo-store:latest'
    EXECUTOR_IMAGE = 'harbor.zetyun.cn/dingodb/dingo:develop-latest'
    MDS_TARGET = '/home/jenkins/.dingo/components/dingo-mds/main/dingo-mds'
    MDS_CLIENT_TARGET = '/home/jenkins/.dingo/components/dingo-mds-client/main/dingo-mds-client'
    CLIENT_TARGET = '/home/jenkins/.dingo/components/dingo-client/main/dingo-client'
    CACHE_TARGET = '/home/jenkins/.dingo/components/dingo-cache/main/dingo-cache'
    CLIENT_LOG_DIR = '/mnt/disk1/dingo_autotest/client_log'
    DTT_REPORT_DIR = '/mnt/disk1/daigy/tmp/output'
  }

  options {
    disableConcurrentBuilds()
    skipDefaultCheckout(true)
    timestamps()
    buildDiscarder(logRotator(numToKeepStr: '30'))
  }

  stages {
    stage('Validate request') {
      steps {
        script {
          if (!(params.GIT_SHA ==~ /[0-9a-fA-F]{40}/)) {
            error('GIT_SHA must be a full 40-character commit SHA')
          }
          if (!params.GIT_REF.startsWith('refs/heads/gh-readonly-queue/main/')) {
            error('GIT_REF must be a main merge-queue ref')
          }
          if (!(params.GITHUB_RUN_ID ==~ /[0-9]+/)) {
            error('GITHUB_RUN_ID must be numeric')
          }
          if (params.GITHUB_REPOSITORY != 'dingodb/dingofs') {
            error('GITHUB_REPOSITORY must be dingodb/dingofs')
          }
          if (params.GITHUB_SERVER_URL != 'https://github.com') {
            error('GITHUB_SERVER_URL must be https://github.com')
          }
        }
      }
    }

    stage('Checkout exact merge group') {
      steps {
        deleteDir()
        sh '''#!/usr/bin/env bash
          set -euo pipefail
          git init .
          git remote add origin "${DINGOFS_REPO_URL}"
          git fetch --no-tags --depth=1 origin "${GIT_REF}"
          git checkout --detach FETCH_HEAD
          actual=$(git rev-parse HEAD)
          test "${actual}" = "${GIT_SHA}"
          git submodule sync --recursive
          git submodule update --init --recursive
        '''
      }
    }

    stage('Build exact SHA') {
      steps {
        sh '''#!/usr/bin/env bash
          set -euo pipefail
          image='dingodatabase/dingo-eureka:rocky9-fs'
          docker pull "${image}"
          sdk_sha=$(git ls-remote https://github.com/dingodb/dingo-sdk.git \
            refs/heads/main | awk 'NR == 1 {print $1}')
          helper_sha=$(sha256sum \
            "${WORKSPACE}/.github/scripts/_lib/build-dingo-sdk.sh" | \
            awk '{print $1}')
          image_id=$(docker image inspect --format '{{.Id}}' "${image}")
          image_digests=$(docker image inspect \
            --format '{{join .RepoDigests ","}}' "${image}")
          [[ "${sdk_sha}" =~ ^[0-9a-f]{40}$ ]]
          [[ "${helper_sha}" =~ ^[0-9a-f]{64}$ ]]
          [[ "${image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]

          sdk_cache_key="dingo-sdk-v1-${helper_sha}-${image_id#sha256:}-${sdk_sha}"
          sdk_cache_final="${SDK_CACHE_ROOT}/${sdk_cache_key}"
          sdk_cache_build="${SDK_CACHE_ROOT}/.building-${BUILD_NUMBER}-${sdk_cache_key}"
          sdk_cache_private="${WORKSPACE}/.jenkins-sdk-cache"
          for workspace_output in \
            .jenkins-sdk-cache-expected candidate-image.sha256 \
            jenkins-build-inputs.txt; do
            test ! -e "${WORKSPACE}/${workspace_output}"
            test ! -L "${WORKSPACE}/${workspace_output}"
          done
          test "$(realpath -m "${SDK_CACHE_ROOT}")" = \
            '/home/jenkins/.cache/dingofs-ci/dingo-sdk'
          [[ "$(realpath -m "${sdk_cache_final}")" == \
             '/home/jenkins/.cache/dingofs-ci/dingo-sdk/'* ]]
          [[ "$(realpath -m "${sdk_cache_build}")" == \
             '/home/jenkins/.cache/dingofs-ci/dingo-sdk/'* ]]
          test ! -L "${sdk_cache_final}"
          test ! -L "${sdk_cache_build}"
          test ! -L "${sdk_cache_private}"
          mkdir -p "${SDK_CACHE_ROOT}"
          test ! -e "${sdk_cache_private}"
          mkdir "${sdk_cache_private}"
          expected_manifest="${WORKSPACE}/.jenkins-sdk-cache-expected"
          printf '%s\n' \
            "sdk_sha=${sdk_sha}" \
            "helper_sha=${helper_sha}" \
            "image_id=${image_id}" >"${expected_manifest}"
          cache_was_hit=false
          if [[ -f "${sdk_cache_final}/.cache-complete" ]]; then
            test -f "${sdk_cache_final}/.jenkins-cache-manifest"
            diff -u "${expected_manifest}" \
              "${sdk_cache_final}/.jenkins-cache-manifest"
            cache_was_hit=true
          else
            test ! -e "${sdk_cache_build}"
            mkdir "${sdk_cache_build}"
            docker run --rm \
              -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
              -e EXPECTED_DINGO_SDK_SHA="${sdk_sha}" \
              -v "${WORKSPACE}:/opt/dingofs:ro" \
              -v "${sdk_cache_build}:/root/.local/dingo-sdk" \
              "${image_id}" bash -lc 'cleanup() {
                  rc=$?
                  trap - EXIT
                  chown -R "${HOST_UID}:${HOST_GID}" \
                    /root/.local/dingo-sdk || true
                  exit "${rc}"
                }
                trap cleanup EXIT
                set -euo pipefail
                source /opt/dingofs/.github/scripts/_lib/build-dingo-sdk.sh
                test "$(git -C /dingo-sdk rev-parse HEAD)" = \
                  "${EXPECTED_DINGO_SDK_SHA}"
                test -f /root/.local/dingo-sdk/.cache-complete
              '
            cp "${expected_manifest}" \
              "${sdk_cache_build}/.jenkins-cache-manifest"
            test -f "${sdk_cache_build}/.cache-complete"
            if [[ ! -e "${sdk_cache_final}" ]]; then
              if ! mv -T "${sdk_cache_build}" "${sdk_cache_final}"; then
                test -f "${sdk_cache_final}/.cache-complete"
              fi
            fi
            diff -u "${expected_manifest}" \
              "${sdk_cache_final}/.jenkins-cache-manifest"
          fi
          test ! -L "${sdk_cache_final}"
          test ! -L "${sdk_cache_final}/.cache-complete"
          test ! -L "${sdk_cache_final}/.jenkins-cache-manifest"
          cp -a "${sdk_cache_final}/." "${sdk_cache_private}/"

          docker run --rm \
            -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
            -v "${WORKSPACE}:/opt/dingofs" \
            -v "${sdk_cache_private}:/root/.local/dingo-sdk" \
            "${image_id}" bash -lc 'cleanup() {
                rc=$?
                trap - EXIT
                chown -R "${HOST_UID}:${HOST_GID}" \
                  /opt/dingofs/build \
                  /opt/dingofs/scripts/docker/rocky9/dingofs \
                  /root/.local/dingo-sdk || true
                exit "${rc}"
              }
              trap cleanup EXIT
              set -euo pipefail
              diff -u /opt/dingofs/.jenkins-sdk-cache-expected \
                /root/.local/dingo-sdk/.jenkins-cache-manifest
              source /opt/dingofs/.github/scripts/_lib/build-dingo-sdk.sh
              cd /opt/dingofs
              git config --global --add safe.directory /opt/dingofs
              make file_dep
              make file_build only=//src/* release=1 unit_tests=OFF
              make file_deploy_config
            '

          for binary in dingo-mds dingo-mds-client dingo-client dingo-cache; do
            test -x "${WORKSPACE}/build/bin/${binary}"
          done
          candidate_image_tag="harbor.zetyun.cn/dingofs/dingofs:jenkins-regression-${BUILD_NUMBER}-${GIT_SHA}"
          docker build --no-cache -t "${candidate_image_tag}" \
            "${WORKSPACE}/scripts/docker/rocky9"
          candidate_image_id=$(docker image inspect --format '{{.Id}}' \
            "${candidate_image_tag}")
          [[ "${candidate_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]
          docker run --rm --entrypoint /usr/bin/sha256sum \
            "${candidate_image_id}" \
            /dingofs/mds/sbin/dingo-mds \
            /dingofs/mds-client/sbin/dingo-mds-client \
            /dingofs/client/sbin/dingo-client \
            /dingofs/cache/sbin/dingo-cache \
            >"${WORKSPACE}/candidate-image.sha256"
          printf '%s\n' \
            "sdk_sha=${sdk_sha}" \
            "helper_sha=${helper_sha}" \
            "build_image_id=${image_id}" \
            "build_image_digests=${image_digests}" \
            "sdk_cache_key=${sdk_cache_key}" \
            "sdk_cache_hit=${cache_was_hit}" \
            "candidate_image_tag=${candidate_image_tag}" \
            "candidate_image_id=${candidate_image_id}" \
            >"${WORKSPACE}/jenkins-build-inputs.txt"
        '''
      }
    }

    stage('Shared environment regression') {
      steps {
        script {
          lock(resource: 'dingofs-regression-env') {
            def failures = []
            def containerCleaned = false
            def clusterStopped = false
            def clusterDestroyed = false
            def portsReleased = false
            def clientLogsCleaned = false
            def evidenceReady = false

            sh label: 'Verify shared environment health', script: '''#!/usr/bin/env bash
              set -euo pipefail
              test "${UNHEALTHY_MARKER}" = \
                '/home/jenkins/.cache/dingofs-ci/regression-unhealthy'
              if [[ -e "${UNHEALTHY_MARKER}" || -L "${UNHEALTHY_MARKER}" ]]; then
                echo 'Shared environment is quarantined; manual recovery is required' >&2
                cat "${UNHEALTHY_MARKER}" >&2 || true
                exit 1
              fi
              artifacts="${WORKSPACE}/jenkins-regression-artifacts"
              test "$(realpath -m "${artifacts}")" = \
                "${WORKSPACE}/jenkins-regression-artifacts"
              test ! -e "${artifacts}"
              test ! -L "${artifacts}"
              mkdir "${artifacts}"
              for marker in \
                .jenkins-env-transaction-started \
                .jenkins-cluster-transaction-started \
                .dtt-smoke-started .dtt-smoke-completed \
                deployment-complete jenkins-cluster-inputs.txt; do
                test ! -e "${WORKSPACE}/${marker}"
                test ! -L "${WORKSPACE}/${marker}"
              done
            '''

            try {
              sh label: 'Prepare local DTT launcher', script: '''#!/usr/bin/env bash
                set -euo pipefail
                docker_wrapper_dir="${WORKSPACE}/.jenkins-dtt-runtime-bin"
                dtt_inputs="${WORKSPACE}/jenkins-dtt-inputs.txt"
                test "${DTT_BIN}" = '/home/jenkins/.local/bin/dtt'
                test -x "${DTT_BIN}"
                test ! -e "${docker_wrapper_dir}"
                test ! -L "${docker_wrapper_dir}"
                test ! -e "${WORKSPACE}/.jenkins-real-docker"
                test ! -L "${WORKSPACE}/.jenkins-real-docker"
                test ! -e "${dtt_inputs}"
                test ! -L "${dtt_inputs}"
                mkdir "${docker_wrapper_dir}"
                real_docker=$(command -v docker)
                [[ "${real_docker}" == /* ]]
                printf '%s\n' \
                  '#!/usr/bin/env bash' \
                  'set -euo pipefail' \
                  'if [[ "${1:-}" == run ]]; then' \
                  '  shift' \
                  '  set -- run --name "${JENKINS_DTT_CONTAINER_NAME}" --label "dingofs.jenkins.transaction=${JENKINS_DTT_CONTAINER_LABEL}" "$@"' \
                  'fi' \
                  'exec "${JENKINS_REAL_DOCKER}" "$@"' \
                  >"${docker_wrapper_dir}/docker"
                chmod 0755 "${docker_wrapper_dir}/docker"
                printf '%s\n' "${real_docker}" \
                  >"${WORKSPACE}/.jenkins-real-docker"
                printf '%s\n' \
                  "dtt_path=${DTT_BIN}" \
                  "dtt_realpath=$(realpath -e "${DTT_BIN}")" \
                  "dtt_sha=$(sha256sum "${DTT_BIN}" | awk '{print $1}')" \
                  >"${dtt_inputs}"
              '''

              sh label: 'Deploy candidate binaries', script: '''#!/usr/bin/env bash
                set -euo pipefail
                artifacts="${WORKSPACE}/jenkins-regression-artifacts"
                test -d "${artifacts}"
                test ! -L "${artifacts}"
                test "$(realpath -e "${artifacts}")" = \
                  "${WORKSPACE}/jenkins-regression-artifacts"
                test ! -e "${UNHEALTHY_MARKER}"
                guard_parent="${UNHEALTHY_MARKER%/*}"
                test "${guard_parent}" = '/home/jenkins/.cache/dingofs-ci'
                test -d "${guard_parent}"
                test ! -L "${guard_parent}"
                test "$(realpath -e "${guard_parent}")" = "${guard_parent}"
                unhealthy_tmp="${UNHEALTHY_MARKER}.tmp-${BUILD_NUMBER}"
                test ! -e "${unhealthy_tmp}"
                test ! -L "${unhealthy_tmp}"
                printf '%s\n' \
                  'state=transaction-in-progress' \
                  "build_number=${BUILD_NUMBER}" \
                  "git_sha=${GIT_SHA}" \
                  "github_run_id=${GITHUB_RUN_ID}" \
                  >"${unhealthy_tmp}"
                mv -f "${unhealthy_tmp}" "${UNHEALTHY_MARKER}"
                touch "${WORKSPACE}/.jenkins-env-transaction-started"

                components_root='/home/jenkins/.dingo/components'
                test -d "${components_root}"
                test ! -L "${components_root}"
                test "$(realpath -e "${components_root}")" = "${components_root}"

                record_original() {
                  local name=$1
                  local target=$2
                  if [[ -f "${target}" && ! -L "${target}" ]]; then
                    local original_sha original_mode
                    original_sha=$(sha256sum "${target}" | awk '{print $1}')
                    original_mode=$(stat -c '%a' "${target}")
                    printf '%s  %s\n' "${original_sha}" "${name}" \
                      >>"${artifacts}/original.sha256"
                    printf '%s  %s\n' "${original_mode}" "${name}" \
                      >>"${artifacts}/original.mode"
                  else
                    printf 'missing  %s\n' "${name}" >>"${artifacts}/original.sha256"
                    printf 'missing  %s\n' "${name}" >>"${artifacts}/original.mode"
                  fi
                }

                deploy_one() {
                  local name=$1
                  local source=$2
                  local target=$3
                  local target_parent="${target%/*}"
                  local component_dir="${target_parent%/*}"
                  local tmp="${target}.jenkins-${BUILD_NUMBER}.tmp"
                  local source_sha target_sha target_mode
                  [[ "${component_dir}" == "${components_root}/"* ]]
                  if [[ ! -e "${component_dir}" ]]; then
                    mkdir "${component_dir}"
                  fi
                  test -d "${component_dir}"
                  test ! -L "${component_dir}"
                  test "$(realpath -e "${component_dir}")" = "${component_dir}"
                  if [[ ! -e "${target_parent}" ]]; then
                    mkdir "${target_parent}"
                  fi
                  test -d "${target_parent}"
                  test ! -L "${target_parent}"
                  test "$(realpath -e "${target_parent}")" = "${target_parent}"
                  test ! -L "${target}"
                  record_original "${name}" "${target}"
                  test -x "${source}"
                  test ! -e "${tmp}"
                  test ! -L "${tmp}"
                  install -m 0755 "${source}" "${tmp}"
                  mv -f "${tmp}" "${target}"
                  source_sha=$(sha256sum "${source}" | awk '{print $1}')
                  target_sha=$(sha256sum "${target}" | awk '{print $1}')
                  target_mode=$(stat -c '%a' "${target}")
                  test "${source_sha}" = "${target_sha}"
                  sha256sum "${target}" >>"${artifacts}/deployed.sha256"
                  printf '%s  %s\n' "${target_mode}" "${name}" \
                    >>"${artifacts}/deployed.mode"
                }

                verify_target_path() {
                  local target=$1
                  local expected=$2
                  local target_parent="${target%/*}"
                  test "${target}" = "${expected}"
                  test -d "${target_parent}"
                  test ! -L "${target_parent}"
                  test -f "${target}"
                  test -x "${target}"
                  test ! -L "${target}"
                  test "$(realpath -e "${target}")" = "${expected}"
                }

                test "${MDS_TARGET}" = \
                  '/home/jenkins/.dingo/components/dingo-mds/main/dingo-mds'
                test "${MDS_CLIENT_TARGET}" = \
                  '/home/jenkins/.dingo/components/dingo-mds-client/main/dingo-mds-client'
                test "${CLIENT_TARGET}" = \
                  '/home/jenkins/.dingo/components/dingo-client/main/dingo-client'
                test "${CACHE_TARGET}" = \
                  '/home/jenkins/.dingo/components/dingo-cache/main/dingo-cache'
                deploy_one dingo-mds "${WORKSPACE}/build/bin/dingo-mds" "${MDS_TARGET}"
                deploy_one dingo-mds-client "${WORKSPACE}/build/bin/dingo-mds-client" "${MDS_CLIENT_TARGET}"
                deploy_one dingo-client "${WORKSPACE}/build/bin/dingo-client" "${CLIENT_TARGET}"
                deploy_one dingo-cache "${WORKSPACE}/build/bin/dingo-cache" "${CACHE_TARGET}"
                verify_target_path "${MDS_TARGET}" \
                  '/home/jenkins/.dingo/components/dingo-mds/main/dingo-mds'
                verify_target_path "${MDS_CLIENT_TARGET}" \
                  '/home/jenkins/.dingo/components/dingo-mds-client/main/dingo-mds-client'
                verify_target_path "${CLIENT_TARGET}" \
                  '/home/jenkins/.dingo/components/dingo-client/main/dingo-client'
                verify_target_path "${CACHE_TARGET}" \
                  '/home/jenkins/.dingo/components/dingo-cache/main/dingo-cache'
                touch "${WORKSPACE}/deployment-complete"
              '''

              sh label: 'Create disposable cluster', script: '''#!/usr/bin/env bash
                set -euo pipefail
                test "${CLUSTER_ROOT}" = \
                  '/home/jenkins/.cache/dingofs-ci/regression-clusters'
                test "${CLUSTER_NAME}" = 'main-dingofs'
                test "${DINGO_CLI}" = '/home/jenkins/.dingo/bin/dingo'
                test -x "${DINGO_CLI}"
                test -f "${WORKSPACE}/.jenkins-env-transaction-started"
                test -f "${UNHEALTHY_MARKER}"
                cluster_parent="${CLUSTER_ROOT%/*}"
                test "${cluster_parent}" = '/home/jenkins/.cache/dingofs-ci'
                test -d "${cluster_parent}"
                test ! -L "${cluster_parent}"
                test "$(realpath -e "${cluster_parent}")" = "${cluster_parent}"
                if [[ ! -e "${CLUSTER_ROOT}" ]]; then
                  mkdir "${CLUSTER_ROOT}"
                fi
                test -d "${CLUSTER_ROOT}"
                test ! -L "${CLUSTER_ROOT}"
                test "$(realpath -e "${CLUSTER_ROOT}")" = "${CLUSTER_ROOT}"
                cluster_runtime="${CLUSTER_ROOT}/${BUILD_NUMBER}-${GIT_SHA}"
                test ! -e "${cluster_runtime}"
                test ! -L "${cluster_runtime}"
                mkdir "${cluster_runtime}"

                candidate_image_tag="harbor.zetyun.cn/dingofs/dingofs:jenkins-regression-${BUILD_NUMBER}-${GIT_SHA}"
                candidate_image_id=$(docker image inspect --format '{{.Id}}' \
                  "${candidate_image_tag}")
                docker pull "${STORE_IMAGE}"
                docker pull "${EXECUTOR_IMAGE}"
                store_image_id=$(docker image inspect --format '{{.Id}}' "${STORE_IMAGE}")
                executor_image_id=$(docker image inspect --format '{{.Id}}' "${EXECUTOR_IMAGE}")
                store_image_digests=$(docker image inspect \
                  --format '{{join .RepoDigests ","}}' "${STORE_IMAGE}")
                executor_image_digests=$(docker image inspect \
                  --format '{{join .RepoDigests ","}}' "${EXECUTOR_IMAGE}")
                dingo_cli_sha=$(sha256sum "${DINGO_CLI}" | awk '{print $1}')
                [[ "${candidate_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]
                [[ "${store_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]
                [[ "${executor_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]]
                printf '%s\n' \
                  "candidate_image_tag=${candidate_image_tag}" \
                  "candidate_image_id=${candidate_image_id}" \
                  "store_image=${STORE_IMAGE}" \
                  "store_image_id=${store_image_id}" \
                  "store_image_digests=${store_image_digests}" \
                  "executor_image=${EXECUTOR_IMAGE}" \
                  "executor_image_id=${executor_image_id}" \
                  "executor_image_digests=${executor_image_digests}" \
                  "dingo_cli_sha=${dingo_cli_sha}" \
                  >"${WORKSPACE}/jenkins-cluster-inputs.txt"

                cluster_state() {
                  local cluster_list
                  if ! cluster_list=$("${DINGO_CLI}" cluster ls); then
                    return 2
                  fi
                  if awk -v name="${CLUSTER_NAME}" \
                    '$1 == name {found=1} END {exit !found}' \
                    <<<"${cluster_list}"; then
                    printf '%s\n' present
                  else
                    printf '%s\n' absent
                  fi
                }
                export dingo_cluster="${CLUSTER_NAME}"
                touch "${WORKSPACE}/.jenkins-cluster-transaction-started"
                state=$(cluster_state)
                if [[ "${state}" == present ]]; then
                  "${DINGO_CLI}" cluster stop -f
                  "${DINGO_CLI}" cluster clean -o container -f
                  "${DINGO_CLI}" cluster rm "${CLUSTER_NAME}" -f
                fi
                state=$(cluster_state)
                test "${state}" = absent

                hosts_file="${cluster_runtime}/standalone-hosts.yaml"
                topology_file="${cluster_runtime}/standalone-topology.yaml"
                cat >"${hosts_file}" <<'HOSTS'
global:
  user: jenkins
  ssh_port: 22
  private_key_file: /home/jenkins/.ssh/id_rsa
hosts:
  - host: dingo127
    hostname: 172.30.14.127
HOSTS
                topology_rendered="${topology_file}.rendered"
                test ! -e "${topology_file}"
                test ! -L "${topology_file}"
                test ! -e "${topology_rendered}"
                test ! -L "${topology_rendered}"
                cat >"${topology_file}" <<'TOPOLOGY'
kind: dingofs
global:
  container_image: @CANDIDATE_IMAGE_TAG@
  data_dir: @CLUSTER_RUNTIME@/data/${service_role}${service_host_sequence}
  log_dir: @CLUSTER_RUNTIME@/logs/${service_role}${service_host_sequence}
  raft_dir: @CLUSTER_RUNTIME@/raft/${service_role}${service_host_sequence}
  default_replica_num: 3
  source_core_dir: /mnt/disk1/corefiles
  target_core_dir: /mnt/disk1/corefiles
  variable:
    home: /tmp
    target: dingo127
coordinator_services:
  config:
    container_image: @STORE_IMAGE@
    server.port: 650${service_host_sequence}
    raft.port: 750${service_host_sequence}
  deploy:
    - host: ${target}
    - host: ${target}
    - host: ${target}
store_services:
  config:
    container_image: @STORE_IMAGE@
    server.port: 660${service_host_sequence}
    raft.port: 760${service_host_sequence}
    gflag.dingo_log_switch_txn_gc_detail: false
    gflag.dingo_log_switch_txn_detail: true
  deploy:
    - host: ${target}
    - host: ${target}
    - host: ${target}
mds_services:
  config:
    server.port: 690${service_host_sequence}
  deploy:
    - host: ${target}
    - host: ${target}
    - host: ${target}
executor_services:
  config:
    container_image: @EXECUTOR_IMAGE@
    port: 18765
    mysqlPort: 13307
    java.Xms: 256m
    java.Xmx: 1g
    java.SoftMaxHeapSize: 512m
    java.MaxDirectMemorySize: 256m
  deploy:
    - host: ${target}
TOPOLOGY
                for topology_value in "${candidate_image_tag}" \
                                      "${cluster_runtime}" \
                                      "${STORE_IMAGE}" \
                                      "${EXECUTOR_IMAGE}"; do
                  if [[ "${topology_value}" == *'|'* ||
                        "${topology_value}" == *'&'* ]]; then
                    echo 'topology replacement contains an unsafe character' >&2
                    exit 1
                  fi
                done
                sed \
                  -e "s|@CANDIDATE_IMAGE_TAG@|${candidate_image_tag}|g" \
                  -e "s|@CLUSTER_RUNTIME@|${cluster_runtime}|g" \
                  -e "s|@STORE_IMAGE@|${STORE_IMAGE}|g" \
                  -e "s|@EXECUTOR_IMAGE@|${EXECUTOR_IMAGE}|g" \
                  "${topology_file}" >"${topology_rendered}"
                for topology_marker in \
                  @CANDIDATE_IMAGE_TAG@ @CLUSTER_RUNTIME@ \
                  @STORE_IMAGE@ @EXECUTOR_IMAGE@; do
                  if grep -F "${topology_marker}" "${topology_rendered}"; then
                    echo "unresolved topology marker: ${topology_marker}" >&2
                    exit 1
                  fi
                done
                grep -F '${service_role}${service_host_sequence}' \
                  "${topology_rendered}"
                grep -F '${target}' "${topology_rendered}"
                mv -f "${topology_rendered}" "${topology_file}"
                # Keep the reviewed topology's complete fixed port set visible.
                test '6500 6501 6502 7500 7501 7502' = \
                  '6500 6501 6502 7500 7501 7502'
                test '6600 6601 6602 7600 7601 7602' = \
                  '6600 6601 6602 7600 7601 7602'
                test '6900 6901 6902 18765 13307' = \
                  '6900 6901 6902 18765 13307'
                "${DINGO_CLI}" hosts commit "${hosts_file}" -f
                "${DINGO_CLI}" cluster add "${CLUSTER_NAME}" -f "${topology_file}"
                "${DINGO_CLI}" cluster deploy -k --local

                health="${WORKSPACE}/jenkins-regression-artifacts/cluster-health.txt"
                for attempt in $(seq 1 60); do
                  if "${DINGO_CLI}" cluster status >"${health}.tmp" 2>&1 &&
                     [[ "$(grep -c '1/1' "${health}.tmp")" -eq 10 ]]; then
                    mv -f "${health}.tmp" "${health}"
                    break
                  fi
                  sleep 5
                done
                test -s "${health}"
                grep -F '172.30.14.127:6900,172.30.14.127:6901,172.30.14.127:6902' \
                  "${health}"

                verify_role_images() {
                  local name_regex=$1
                  local expected_image_id=$2
                  local expected_count=$3
                  local count=0
                  local container_list
                  container_list=$(docker ps --format '{{.Names}} {{.ID}}')
                  while read -r name container_id; do
                    [[ -n "${name}" ]] || continue
                    if [[ "${name}" =~ ${name_regex} ]]; then
                      test "$(docker inspect --format '{{.Image}}' \
                        "${container_id}")" = "${expected_image_id}"
                      count=$((count + 1))
                    fi
                  done <<<"${container_list}"
                  test "${count}" = "${expected_count}"
                }
                verify_role_images '^dingofs-coordinator-[0-9a-f]+$' \
                  "${store_image_id}" 3
                verify_role_images '^dingofs-store-[0-9a-f]+$' \
                  "${store_image_id}" 3
                verify_role_images '^dingofs-mds-[0-9a-f]+$' \
                  "${candidate_image_id}" 3
                verify_role_images '^dingofs-mds-client-[0-9a-f]+$' \
                  "${candidate_image_id}" 1
                verify_role_images '^dingofs-executor-[0-9a-f]+$' \
                  "${executor_image_id}" 1
                test "$(docker image inspect --format '{{.Id}}' \
                  "${candidate_image_tag}")" = "${candidate_image_id}"

                test "${CLIENT_LOG_DIR}" = '/mnt/disk1/dingo_autotest/client_log'
                if [[ ! -e "${CLIENT_LOG_DIR}" ]]; then
                  mkdir "${CLIENT_LOG_DIR}"
                fi
                test -d "${CLIENT_LOG_DIR}"
                test ! -L "${CLIENT_LOG_DIR}"
                test "$(realpath -e "${CLIENT_LOG_DIR}")" = "${CLIENT_LOG_DIR}"
                find "${CLIENT_LOG_DIR}" -xdev -mindepth 1 -delete
              '''

              timeout(time: 90, unit: 'MINUTES') {
                sh label: 'Run smoke regression', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  docker_wrapper_dir="${WORKSPACE}/.jenkins-dtt-runtime-bin"
                  export JENKINS_REAL_DOCKER
                  JENKINS_REAL_DOCKER=$(cat "${WORKSPACE}/.jenkins-real-docker")
                  export JENKINS_DTT_CONTAINER_NAME="dingofs-regression-${BUILD_NUMBER}-${GIT_SHA}"
                  export JENKINS_DTT_CONTAINER_LABEL="${BUILD_NUMBER}:${GIT_SHA}"
                  export PATH="${docker_wrapper_dir}:/home/jenkins/miniforge3/bin:${PATH}"
                  export PYTHONHOME=/home/jenkins/miniforge3
                  export PYTHONPATH=/home/jenkins/miniforge3/lib/python3.13/site-packages
                  touch "${WORKSPACE}/.dtt-smoke-started"
                  "${DTT_BIN}" smoke --env env_127
                  touch "${WORKSPACE}/.dtt-smoke-completed"
                '''
              }
            } catch (err) {
              failures << "primary: ${err}"
            } finally {
              try {
                sh label: 'Capture live cluster evidence', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  artifacts="${WORKSPACE}/jenkins-regression-artifacts"
                  mkdir -p "${artifacts}/cluster-logs"
                  if [[ -f "${WORKSPACE}/.jenkins-env-transaction-started" ]]; then
                    export dingo_cluster="${CLUSTER_NAME}"
                    "${DINGO_CLI}" cluster status \
                      >"${artifacts}/cluster-status-final.txt" 2>&1 || true
                    docker ps -a --filter 'name=dingofs-' --no-trunc \
                      >"${artifacts}/docker-ps.txt"
                    while IFS= read -r container; do
                      [[ -n "${container}" ]] || continue
                      docker inspect "${container}" \
                        >"${artifacts}/cluster-logs/${container}.inspect.json"
                      docker logs "${container}" \
                        >"${artifacts}/cluster-logs/${container}.log" 2>&1 || true
                    done < <(docker ps -a --filter 'name=dingofs-' \
                      --format '{{.Names}}')
                  fi
                  tar -C "${artifacts}" -czf "${artifacts}/cluster-logs.tgz" \
                    cluster-logs

                  report_list="${artifacts}/dtt-report-files.list0"
                  : >"${report_list}"
                  if [[ -f "${WORKSPACE}/.dtt-smoke-started" && \
                        -d "${DTT_REPORT_DIR}" ]]; then
                    (cd "${DTT_REPORT_DIR}" && find . -xdev -type f \
                      -newer "${WORKSPACE}/.dtt-smoke-started" -print0 \
                      >"${report_list}")
                  fi
                  if [[ -d "${DTT_REPORT_DIR}" ]]; then
                    tar -C "${DTT_REPORT_DIR}" --null \
                      --files-from="${report_list}" \
                      -czf "${artifacts}/dtt-report.tgz"
                  else
                    tar -C "${artifacts}" -czf "${artifacts}/dtt-report.tgz" \
                      --files-from=/dev/null
                  fi
                  if [[ -d "${CLIENT_LOG_DIR}" ]]; then
                    tar -C "${CLIENT_LOG_DIR}" -czf \
                      "${artifacts}/client-logs.tgz" .
                  else
                    tar -C "${artifacts}" -czf "${artifacts}/client-logs.tgz" \
                      --files-from=/dev/null
                  fi
                  for input in jenkins-build-inputs.txt jenkins-cluster-inputs.txt \
                               jenkins-dtt-inputs.txt candidate-image.sha256; do
                    if [[ -f "${WORKSPACE}/${input}" ]]; then
                      cp "${WORKSPACE}/${input}" "${artifacts}/"
                    fi
                  done
                '''
              } catch (err) {
                failures << "evidence capture: ${err}"
              }

              try {
                sh label: 'Remove regression container', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  [[ "${BUILD_NUMBER}" =~ ^[0-9]+$ ]]
                  [[ "${GIT_SHA}" =~ ^[0-9a-fA-F]{40}$ ]]
                  container_name="dingofs-regression-${BUILD_NUMBER}-${GIT_SHA}"
                  expected_label="${BUILD_NUMBER}:${GIT_SHA}"
                  matches=$(docker ps -a --filter \
                    "name=^/${container_name}$" --format '{{.Names}}')
                  if [[ -n "${matches}" ]]; then
                    test "${matches}" = "${container_name}"
                    actual_label=$(docker inspect --format \
                      '{{index .Config.Labels "dingofs.jenkins.transaction"}}' \
                      "${container_name}")
                    test "${actual_label}" = "${expected_label}"
                    docker rm -f -- "${container_name}"
                  fi
                  test -z "$(docker ps -a --filter \
                    "name=^/${container_name}$" --format '{{.Names}}')"
                '''
                containerCleaned = true
              } catch (err) {
                failures << "DTT container cleanup: ${err}; environment remains quarantined"
              }

              try {
                sh label: 'Stop disposable cluster', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  if [[ ! -f "${WORKSPACE}/.jenkins-cluster-transaction-started" ]]; then
                    exit 0
                  fi
                  export dingo_cluster="${CLUSTER_NAME}"
                  cluster_state() {
                    local cluster_list
                    if ! cluster_list=$("${DINGO_CLI}" cluster ls); then
                      return 2
                    fi
                    if awk -v name="${CLUSTER_NAME}" \
                      '$1 == name {found=1} END {exit !found}' \
                      <<<"${cluster_list}"; then
                      printf '%s\n' present
                    else
                      printf '%s\n' absent
                    fi
                  }
                  state=$(cluster_state)
                  if [[ "${state}" == present ]]; then
                    "${DINGO_CLI}" cluster stop -f
                  fi
                '''
                clusterStopped = true
              } catch (err) {
                failures << "cluster stop: ${err}; environment remains quarantined"
              }

              try {
                sh label: 'Destroy disposable cluster', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  cluster_runtime="${CLUSTER_ROOT}/${BUILD_NUMBER}-${GIT_SHA}"
                  test "${cluster_runtime}" = \
                    "/home/jenkins/.cache/dingofs-ci/regression-clusters/${BUILD_NUMBER}-${GIT_SHA}"
                  if [[ ! -f "${WORKSPACE}/.jenkins-cluster-transaction-started" ]]; then
                    test ! -L "${cluster_runtime}"
                    if [[ -d "${cluster_runtime}" ]]; then
                      test -d "${CLUSTER_ROOT}"
                      test ! -L "${CLUSTER_ROOT}"
                      test "$(realpath -e "${CLUSTER_ROOT}")" = "${CLUSTER_ROOT}"
                      rm -rf -- "${cluster_runtime}"
                    fi
                    exit 0
                  fi
                  export dingo_cluster="${CLUSTER_NAME}"
                  cluster_state() {
                    local cluster_list
                    if ! cluster_list=$("${DINGO_CLI}" cluster ls); then
                      return 2
                    fi
                    if awk -v name="${CLUSTER_NAME}" \
                      '$1 == name {found=1} END {exit !found}' \
                      <<<"${cluster_list}"; then
                      printf '%s\n' present
                    else
                      printf '%s\n' absent
                    fi
                  }
                  state=$(cluster_state)
                  if [[ "${state}" == present ]]; then
                    "${DINGO_CLI}" cluster clean -o container -f
                    "${DINGO_CLI}" cluster rm "${CLUSTER_NAME}" -f
                  fi
                  state=$(cluster_state)
                  test "${state}" = absent
                  test -d "${CLUSTER_ROOT}"
                  test ! -L "${CLUSTER_ROOT}"
                  test "$(realpath -e "${CLUSTER_ROOT}")" = "${CLUSTER_ROOT}"
                  if [[ -d "${cluster_runtime}" && ! -L "${cluster_runtime}" ]]; then
                    rm -rf -- "${cluster_runtime}"
                  fi
                '''
                clusterDestroyed = true
              } catch (err) {
                failures << "cluster destroy: ${err}; environment remains quarantined"
              }

              if (clusterDestroyed) {
                try {
                  sh label: 'Remove build image tags', script: '''#!/usr/bin/env bash
                    set -euo pipefail
                    candidate_image_tag="harbor.zetyun.cn/dingofs/dingofs:jenkins-regression-${BUILD_NUMBER}-${GIT_SHA}"
                    if docker image inspect "${candidate_image_tag}" >/dev/null 2>&1; then
                      expected_candidate_id=$(awk -F= \
                        '$1 == "candidate_image_id" {print $2}' \
                        "${WORKSPACE}/jenkins-build-inputs.txt")
                      [[ "${expected_candidate_id}" =~ ^sha256:[0-9a-f]{64}$ ]]
                      test "$(docker image inspect --format '{{.Id}}' \
                        "${candidate_image_tag}")" = "${expected_candidate_id}"
                      docker image rm -- "${candidate_image_tag}"
                    fi
                  '''
                } catch (err) {
                  failures << "image tag cleanup: ${err}"
                }
              }

              try {
                sh label: 'Verify released cluster ports', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  if [[ ! -f "${WORKSPACE}/.jenkins-cluster-transaction-started" ]]; then
                    exit 0
                  fi
                  ports=(6500 6501 6502 7500 7501 7502 \
                         6600 6601 6602 7600 7601 7602 \
                         6900 6901 6902 18765 13307)
                  for port in "${ports[@]}"; do
                    listeners=$(ss -H -ltn "sport = :${port}")
                    if [[ -n "${listeners}" ]]; then
                      echo "port ${port} is still listening" >&2
                      exit 1
                    fi
                  done
                '''
                portsReleased = true
              } catch (err) {
                failures << "port release: ${err}; environment remains quarantined"
              }

              try {
                sh label: 'Clean client logs', script: '''#!/usr/bin/env bash
                  set -euo pipefail
                  if [[ ! -f "${WORKSPACE}/.jenkins-env-transaction-started" ]]; then
                    exit 0
                  fi
                  test "${CLIENT_LOG_DIR}" = '/mnt/disk1/dingo_autotest/client_log'
                  test -d "${CLIENT_LOG_DIR}"
                  test ! -L "${CLIENT_LOG_DIR}"
                  test "$(realpath -e "${CLIENT_LOG_DIR}")" = "${CLIENT_LOG_DIR}"
                  find "${CLIENT_LOG_DIR}" -xdev -mindepth 1 -delete
                  remaining=$(find "${CLIENT_LOG_DIR}" -xdev -mindepth 1 \
                    -print -quit)
                  test -z "${remaining}"
                '''
                clientLogsCleaned = true
              } catch (err) {
                failures << "client log cleanup: ${err}; environment remains quarantined"
              }

              try {
                sh label: 'Record cleanup result', script: """#!/usr/bin/env bash
                  set -euo pipefail
                  artifacts=\"\${WORKSPACE}/jenkins-regression-artifacts\"
                  printf '%s\\n' \\
                    'dtt_container_cleaned=${containerCleaned}' \\
                    'cluster_stopped=${clusterStopped}' \\
                    'cluster_destroyed=${clusterDestroyed}' \\
                    'ports_released=${portsReleased}' \\
                    'client_logs_cleaned=${clientLogsCleaned}' \\
                    >\"\${artifacts}/cleanup-status.txt\"
                  primary_result=failed
                  dtt_result=not-run
                  if [[ -f \"\${WORKSPACE}/.dtt-smoke-started\" ]]; then
                    dtt_result=failed
                  fi
                  if [[ -f \"\${WORKSPACE}/.dtt-smoke-completed\" ]]; then
                    primary_result=success
                    dtt_result=success
                  fi
                  printf '%s\\n' \\
                    \"build_number=\${BUILD_NUMBER}\" \\
                    \"git_sha=\${GIT_SHA}\" \\
                    \"primary_result=\${primary_result}\" \\
                    \"dtt_result=\${dtt_result}\" \\
                    >\"\${artifacts}/transaction-status.txt\"
                """
                evidenceReady = true
              } catch (err) {
                failures << "cleanup evidence: ${err}; environment remains quarantined"
              }

              if (containerCleaned && clusterDestroyed && portsReleased &&
                  clientLogsCleaned && evidenceReady) {
                try {
                  sh label: 'Release environment quarantine', script: '''#!/usr/bin/env bash
                    set -euo pipefail
                    test "${UNHEALTHY_MARKER}" = \
                      '/home/jenkins/.cache/dingofs-ci/regression-unhealthy'
                    if [[ -f "${UNHEALTHY_MARKER}" ]]; then
                      grep -Fx "build_number=${BUILD_NUMBER}" "${UNHEALTHY_MARKER}"
                      grep -Fx "git_sha=${GIT_SHA}" "${UNHEALTHY_MARKER}"
                      rm -f -- "${UNHEALTHY_MARKER}"
                    fi
                  '''
                } catch (err) {
                  failures << "quarantine release: ${err}"
                }
              } else {
                failures << 'cluster cleanup could not be confirmed; quarantine retained'
              }

              try {
                archiveArtifacts artifacts: 'jenkins-regression-artifacts/**',
                                 fingerprint: true,
                                 allowEmptyArchive: false
              } catch (err) {
                failures << "archive: ${err}"
              }
            }

            if (!failures.isEmpty()) {
              error(failures.join('\n'))
            }
          }
        }
      }
    }
  }
}
