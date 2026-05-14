#!/bin/bash
# Deploy dingofs MDS + create fs + mount client.
# Self-contained: uses only ${GITHUB_WORKSPACE}/scripts/dev-mds/ templates +
# binaries from ${GITHUB_WORKSPACE}/build/bin/. No monorepo / no $HOME paths.
set -euo pipefail

: "${GITHUB_WORKSPACE:?need GITHUB_WORKSPACE}"
: "${RUNNER_TEMP:?need RUNNER_TEMP}"

# ── Constants ──────────────────────────────────────────────────────────────
COORDINATOR_ADDR="127.0.0.1:32001"
S3_ENDPOINT="http://127.0.0.1:9000"
S3_AK="admin"
S3_SK="admin123"
S3_BUCKET="${S3_BUCKET:-dingofs-ci}"
FS_NAME="${FS_NAME:-ci-e2e}"
CLUSTER_ID=101
MDS_PORT=8821
MDS_INSTANCE_ID=1001
VFS_DUMMY_PORT=10001

BUILD_DIR="${GITHUB_WORKSPACE}/build/bin"
TEMPLATE="${GITHUB_WORKSPACE}/scripts/dev-mds/mds.template.conf"
RUNTIME_DIR="${RUNNER_TEMP}/dingofs-runtime"
MDS_DIST="${RUNTIME_DIR}/mds/dist/mds-1"
CLIENT_LOG="${RUNTIME_DIR}/client/log"
MOUNT_POINT="${RUNNER_TEMP}/dingofs-mount"

# ── Sanity ─────────────────────────────────────────────────────────────────
test -x "${BUILD_DIR}/dingo-mds"        || { echo "✗ no dingo-mds at ${BUILD_DIR}"; exit 1; }
test -x "${BUILD_DIR}/dingo-mds-client" || { echo "✗ no dingo-mds-client at ${BUILD_DIR}"; exit 1; }
test -x "${BUILD_DIR}/dingo-client"     || { echo "✗ no dingo-client at ${BUILD_DIR}"; exit 1; }
test -f "${TEMPLATE}"                   || { echo "✗ no mds template at ${TEMPLATE}"; exit 1; }
command -v dingo >/dev/null             || { echo "✗ dingocli not installed"; exit 1; }

# ── Step 1: Layout ─────────────────────────────────────────────────────────
echo "[deploy] preparing dirs..."
mkdir -p "${MDS_DIST}"/{bin,conf,log} "${CLIENT_LOG}" "${MOUNT_POINT}"

# ── Step 2: Deploy MDS binary ──────────────────────────────────────────────
echo "[deploy] copying mds binaries..."
cp "${BUILD_DIR}/dingo-mds"        "${MDS_DIST}/bin/"
cp "${BUILD_DIR}/dingo-mds-client" "${MDS_DIST}/bin/"

# ── Step 3: Render mds.conf from template ──────────────────────────────────
echo "[deploy] rendering mds.conf..."
DIST_CONF="${MDS_DIST}/conf/mds.conf"
cp "${TEMPLATE}" "${DIST_CONF}"
sed -i "s|\\\$CLUSTER_ID|${CLUSTER_ID}|g"          "${DIST_CONF}"
sed -i "s|\\\$INSTANCE_ID|${MDS_INSTANCE_ID}|g"    "${DIST_CONF}"
sed -i "s|\\\$SERVER_HOST|127.0.0.1|g"             "${DIST_CONF}"
sed -i "s|\\\$SERVER_LISTEN_HOST|127.0.0.1|g"      "${DIST_CONF}"
sed -i "s|\\\$SERVER_PORT|${MDS_PORT}|g"           "${DIST_CONF}"
sed -i "s|\\\$BASE_PATH|${MDS_DIST}|g"             "${DIST_CONF}"
sed -i "s|\\\$STORAGE_ENGINE|dingo-store|g"        "${DIST_CONF}"
sed -i "s|\\\$STORAGE_URL|list://${COORDINATOR_ADDR}|g" "${DIST_CONF}"

echo "${COORDINATOR_ADDR}" > "${MDS_DIST}/conf/coor_list"

# Detect unresolved $VARS (excluding comments)
UNRESOLVED=$(grep -nE '\$[A-Z_]+' "${DIST_CONF}" | grep -v '^\s*#' || true)
if [ -n "${UNRESOLVED}" ]; then
  echo "[deploy] ⚠ unresolved vars in mds.conf:"
  echo "${UNRESOLVED}" | head -5
fi

# ── Step 4: CreateAllTable on dingo-store ──────────────────────────────────
echo "[deploy] CreateAllTable on dingo-store cluster..."
# --mds_storage_dingodb_replica_num=1 是必须的：dingo-mds-client 默认 replica=3
# (gflag in src/mds/storage/dingodb_storage.cc:32)；1+1 拓扑只有 1 个 store，
# 不覆盖会报 "Not enough stores for create region" (errno 60002) 然后 dingo-mds 启动 crash.
OUTPUT=$("${MDS_DIST}/bin/dingo-mds-client" \
  --coor_addr="list://${COORDINATOR_ADDR}" \
  --cluster_id="${CLUSTER_ID}" \
  --cmd=CreateAllTable \
  --storage_engine=dingo-store \
  --mds_storage_dingodb_replica_num=1 2>&1) || true
echo "${OUTPUT}" | head -8
if echo "${OUTPUT}" | grep -qiE "success|already exist|exist|overlaping"; then
  echo "[deploy]   ✓ tables ready"
else
  echo "[deploy] ⚠ unexpected output — MDS start step will validate"
fi

# ── Step 5: Start MDS ──────────────────────────────────────────────────────
echo "[deploy] starting dingo-mds..."
cd "${MDS_DIST}"
ulimit -n 1048576 2>/dev/null || true
# mds_storage_dingodb_replica_num=1 同样要传给 dingo-mds（创建 per-fs region 时也用）
nohup ./bin/dingo-mds --conf=./conf/mds.conf \
  --mds_storage_dingodb_replica_num=1 \
  > ./log/out 2>&1 &
MDS_PID=$!
echo ${MDS_PID} > "${RUNTIME_DIR}/mds.pid"

echo "[deploy] waiting for MDS :${MDS_PORT}..."
for i in {1..30}; do
  if (echo > /dev/tcp/127.0.0.1/${MDS_PORT}) 2>/dev/null; then
    echo "[deploy]   ✓ MDS up (${i}s)  PID=${MDS_PID}"; break
  fi
  [ $i -eq 30 ] && {
    echo "[deploy] ✗ MDS timeout"; tail -30 "${MDS_DIST}/log/out"; exit 1
  }
  sleep 1
done

# ── Step 6: Create fs via dingocli ─────────────────────────────────────────
echo "[deploy] checking fs '${FS_NAME}'..."
DINGO_YAML="${RUNTIME_DIR}/dingo.yaml"
cat > "${DINGO_YAML}" <<EOF
dingofs:
  mdsAddr: 127.0.0.1:${MDS_PORT}
EOF

if CONF="${DINGO_YAML}" dingo fs query --fsname "${FS_NAME}" > /dev/null 2>&1; then
  echo "[deploy]   fs '${FS_NAME}' already exists"
else
  echo "[deploy]   creating fs '${FS_NAME}'..."
  CONF="${DINGO_YAML}" dingo fs create "${FS_NAME}" \
    --storagetype s3 \
    --s3.ak "${S3_AK}" \
    --s3.sk "${S3_SK}" \
    --s3.endpoint "${S3_ENDPOINT}" \
    --s3.bucketname "${S3_BUCKET}"
fi

# ── Step 7: Mount client ───────────────────────────────────────────────────
echo "[deploy] mounting dingo-client at ${MOUNT_POINT}..."
META_URL="mds://127.0.0.1:${MDS_PORT}/${FS_NAME}"
sudo bash -c "
  ulimit -c unlimited
  ulimit -n 1048576
  '${BUILD_DIR}/dingo-client' '${META_URL}' '${MOUNT_POINT}' \
    --log_dir='${CLIENT_LOG}' \
    --log_level=INFO \
    --vfs_access_logging=false \
    --vfs_meta_access_logging=false \
    --s3_loglevel=0 \
    --vfs_write_buffer_total_mb=256 \
    --vfs_dummy_server_port=${VFS_DUMMY_PORT} \
    --cache_store=none \
    --daemonize=true \
    > '${CLIENT_LOG}/stdout.log' 2>&1
"

echo "[deploy] waiting for mount..."
for i in {1..20}; do
  if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
    # Daemonize forks the real client process — sudo + bash wrapper exit. Wait a
    # beat then pgrep on the binary's full path (avoids matching the wrapper).
    sleep 1
    CLIENT_PID=$(sudo pgrep -fx "${BUILD_DIR}/dingo-client .*${FS_NAME}.*" | head -1 || true)
    if [ -z "${CLIENT_PID}" ]; then
      # Fallback: use fuser -m on the mount point
      CLIENT_PID=$(sudo fuser -m "${MOUNT_POINT}" 2>/dev/null | awk '{print $1}' | head -1 || true)
    fi
    echo "${CLIENT_PID}" > "${RUNTIME_DIR}/client.pid"
    echo "[deploy]   ✓ mounted (${i}s)  PID=${CLIENT_PID}"
    break
  fi
  [ $i -eq 20 ] && {
    echo "[deploy] ✗ mount timeout"
    tail -30 "${CLIENT_LOG}/stdout.log" 2>/dev/null || true
    ls -la "${CLIENT_LOG}/" 2>/dev/null || true
    exit 1
  }
  sleep 1
done

echo "[deploy] ✓ all up"
