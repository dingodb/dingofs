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
# Define the BASE_FLAGS and DINGO_DEFAULT_COPTS variables

file(GLOB METRICS_LIB_SRCS
    "common/*.cpp"
    "blockcache/*.cpp"
    "cachegroup/*.cpp"
    "remotecache/*.cpp"
)

add_library(cache_metrics ${METRICS_LIB_SRCS})
target_link_libraries(cache_metrics
    brpc::brpc
)
