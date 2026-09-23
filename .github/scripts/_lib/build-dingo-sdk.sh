#!/bin/bash
# Build & install dingo-sdk into /root/.local/dingo-sdk.
#
# Source this script in native build containers or execute it in cibuildwheel before-all.
# This is the shared dingo-sdk build recipe for unit tests, releases, and wheels.
#
# cache key 通过 `hashFiles('.github/scripts/_lib/build-dingo-sdk.sh')` 指纹本文件：
# 改任何 cmake flag / 编译命令 → hash 变 → cache key 失效 → 自动重编，无需手动 bump。
set -e

if [ -f /root/.local/dingo-sdk/.cache-complete ]; then
  echo "dingo-sdk cache hit — skip build"
else
  (
    # Use fresh sources even when the wheel image already contains an SDK install.
    sdk_source_dir=$(mktemp -d /tmp/dingo-sdk.XXXXXX)
    git clone --branch v1.2 --single-branch https://github.com/dingodb/dingo-sdk.git "$sdk_source_dir"
    cd "$sdk_source_dir"
    git checkout --detach 1d6c98788531ab7fb1112b39d6447d0b221fa6cb
    git submodule sync --recursive
    git submodule update --init --recursive
    mkdir build
    cd build
    cmake \
      -DTHIRD_PARTY_INSTALL_PATH=/root/.local/dingo-eureka \
      -DCMAKE_INSTALL_PREFIX=/root/.local/dingo-sdk \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      -DDINGOSDK_INSTALL=ON \
      -DBUILD_SDK_EXAMPLE=OFF -DBUILD_BENCHMARK=OFF -DBUILD_PYTHON_SDK=OFF \
      -DBUILD_INTEGRATION_TESTS=OFF -DBUILD_UNIT_TESTS=OFF ..
    make -j"$(nproc)"
    make install
    touch /root/.local/dingo-sdk/.cache-complete
  )
fi
export DINGOSDK_INSTALL_PATH=/root/.local/dingo-sdk
