# Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The thin ibverbs wrapper (namespace verbs): RAII over device, QP, CQ and
# MR, no policy. The rdma transport is its only intended user.
file(GLOB BLOCKCACHE_INFINIBAND_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(blockcache_infiniband ${BLOCKCACHE_INFINIBAND_SRCS})

target_compile_features(blockcache_infiniband PRIVATE cxx_std_20)

target_link_libraries(blockcache_infiniband
    ibverbs::ibverbs

    blockcache_status
    absl::cleanup
)

blockcache_tune(blockcache_infiniband)
