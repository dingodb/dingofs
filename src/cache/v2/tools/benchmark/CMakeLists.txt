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

add_executable(cb-v2
    benchmarker.cc
    collector.cc
    factory.cc
    main.cc
    option.cc
    reporter.cc
    worker.cc
)
target_compile_features(cb-v2 PRIVATE cxx_std_20)
target_link_libraries(cb-v2
    ibverbs::ibverbs

    cache_v2_api
    cache_v2_object
    cache_v2_common
    dingofs_executor
    fmt::fmt
    brpc::brpc
    gflags::gflags
    glog::glog
)
set_target_properties(cb-v2 PROPERTIES
    OUTPUT_NAME cb
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/v2
)
cache_tune(cb-v2)
