#!/bin/bash
# Build & install dingo-sdk into /root/.local/dingo-sdk.
#
# 注意：本脚本在 dingo-eureka 容器内被 `source`（由 pr-check 的 test.sh / composite action 的
# build.sh 调用），不是 host 侧 helper。这里是 dingo-sdk 的"构建配方"单一真相源——
# unit-test 和 release build 共用同一份，避免两边 drift。
#
# cache key 通过 `hashFiles('.github/scripts/_lib/build-dingo-sdk.sh')` 指纹本文件：
# 改任何 cmake flag / 编译命令 → hash 变 → cache key 失效 → 自动重编，无需手动 bump。
set -e

if [ -f /root/.local/dingo-sdk/.cache-complete ]; then
  echo "dingo-sdk cache hit — skip build"
else
  cd / && git clone --branch main --single-branch https://github.com/dingodb/dingo-sdk.git && cd dingo-sdk && \
  git submodule sync --recursive && git submodule update --init --recursive && \
  mkdir build && cd build && cmake \
    -DTHIRD_PARTY_INSTALL_PATH=/root/.local/dingo-eureka \
    -DCMAKE_INSTALL_PREFIX=/root/.local/dingo-sdk \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DBUILD_SDK_EXAMPLE=OFF -DBUILD_BENCHMARK=OFF -DBUILD_PYTHON_SDK=OFF \
    -DBUILD_INTEGRATION_TESTS=OFF -DBUILD_UNIT_TESTS=OFF .. && \
  make -j"$(nproc)" && make install && touch /root/.local/dingo-sdk/.cache-complete
fi
export DINGOSDK_INSTALL_PATH=/root/.local/dingo-sdk
