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

# The transport-neutral layer: vocabulary headers only, nothing that knows a
# wire. Each transport is a target of its own underneath -- net/rdma links
# ibverbs, net/brpc links brpc -- so a user of this layer alone links
# neither.
add_library(cache_v2_net INTERFACE)

target_link_libraries(cache_v2_net INTERFACE
    cache_v2_core
)

add_subdirectory(rdma)
add_subdirectory(brpc)
