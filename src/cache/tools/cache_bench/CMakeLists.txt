# Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

add_library(cache_bench_lib
    operation.cc
    options.cc
    reporter.cc
    runner.cc
    stats.cc
)

target_link_libraries(cache_bench_lib
    cache_tier
    cache_common
    block_accesser
    dingofs_common
    dingofs_options
    dingofs_executor

    brpc::brpc
    fmt::fmt
    gflags::gflags
    glog::glog
)

add_executable(cache-bench main.cc)
target_link_libraries(cache-bench
    cache_bench_lib
)
