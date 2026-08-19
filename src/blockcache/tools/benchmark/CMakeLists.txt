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

add_executable(blockcache-cb
    benchmarker.cc
    collector.cc
    factory.cc
    main.cc
    option.cc
    reporter.cc
    worker.cc
)
target_compile_features(blockcache-cb PRIVATE cxx_std_20)
target_link_libraries(blockcache-cb
    ibverbs::ibverbs

    blockcache_api
    blockcache_object
    blockcache_common
    blockcache_flags
    dingofs_executor
    fmt::fmt
    brpc::brpc
    gflags::gflags
    glog::glog
)
set_target_properties(blockcache-cb PROPERTIES
    OUTPUT_NAME cb
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/blockcache
)
blockcache_tune(blockcache-cb)
