#!/bin/bash
# Print sha256 digest for a docker image. Use to bump pinned digests in
# docker-compose.yml.
#
# Usage:
#   bash .github/scripts/get-image-digest.sh dingodatabase/dingo-store latest
#   bash .github/scripts/get-image-digest.sh minio/minio latest
#
# Output: just the digest line, e.g. sha256:6f15f67daf...
# Then update docker-compose.yml with image: <repo>@<digest>.
set -euo pipefail

IMAGE="${1:?usage: get-image-digest.sh <repo> <tag>}"
TAG="${2:?usage: get-image-digest.sh <repo> <tag>}"

# Use docker pull to get the **manifest list / OCI index** digest (multi-arch
# safe — `docker pull foo@<index_digest>` auto-selects the right arch).
# Trade-off: actually pulls the image (~time), but ensures the digest format
# matches what `docker pull` cmd line uses.
DIGEST=$(docker pull "${IMAGE}:${TAG}" 2>&1 | grep -oE 'sha256:[a-f0-9]{64}' | head -1)

if [ -z "${DIGEST}" ] || [[ ! "${DIGEST}" =~ ^sha256:[a-f0-9]{64}$ ]]; then
  echo "✗ failed to extract digest from manifest" >&2
  echo "  raw output:" >&2
  docker manifest inspect --verbose "${IMAGE}:${TAG}" 2>&1 | head -20 >&2
  exit 1
fi

echo "${DIGEST}"
