#!/bin/bash
# Bring up CI stack (minio + 1 coord + 1 store) and create S3 bucket.
# After this returns 0, the dingo-store cluster is registered and bucket exists.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

S3_BUCKET="${S3_BUCKET:-dingofs-ci}"

echo "[up] docker compose up -d"
docker compose up -d

echo "[up] waiting for minio :9000..."
for i in {1..30}; do
  if curl -fs http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then
    echo "[up]   ✓ minio ready (${i}s)"; break
  fi
  [ $i -eq 30 ] && { echo "[up] ✗ minio timeout"; docker compose logs minio | tail -20; exit 1; }
  sleep 1
done

echo "[up] creating bucket '${S3_BUCKET}'..."
docker exec dingofs-ci-minio mc alias set local http://127.0.0.1:9000 admin admin123 >/dev/null
docker exec dingofs-ci-minio mc mb --ignore-existing "local/${S3_BUCKET}"

echo "[up] waiting for coordinator :32001 listen..."
for i in {1..60}; do
  if (echo > /dev/tcp/127.0.0.1/32001) 2>/dev/null; then
    echo "[up]   ✓ coordinator listening (${i}s)"; break
  fi
  [ $i -eq 60 ] && {
    echo "[up] ✗ coord timeout"; docker compose logs coordinator | tail -30; exit 1
  }
  sleep 1
done

echo "[up] waiting for store→coord HEARTBEAT (glog inside container)..."
for i in {1..60}; do
  if docker exec dingofs-ci-coord bash -c \
     "grep -q 'NORMAL HEARTBEAT.*STORE_NORMAL' /opt/dingo-store/dist/coordinator1/log/coordinator.INFO 2>/dev/null"; then
    echo "[up]   ✓ store registered (${i}s)"; break
  fi
  [ $i -eq 60 ] && {
    echo "[up] ✗ HEARTBEAT not seen"
    docker exec dingofs-ci-coord tail -40 /opt/dingo-store/dist/coordinator1/log/coordinator.INFO 2>&1 || true
    docker exec dingofs-ci-store  tail -40 /opt/dingo-store/dist/store1/log/store.INFO 2>&1 || true
    exit 1
  }
  sleep 1
done

echo "[up] ✓ stack ready"
echo "      COORDINATOR_ADDR=127.0.0.1:32001"
echo "      S3_ENDPOINT=http://127.0.0.1:9000 AK=admin SK=admin123 BUCKET=${S3_BUCKET}"
