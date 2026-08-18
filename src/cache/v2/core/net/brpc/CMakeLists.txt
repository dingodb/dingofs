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

# The brpc transport, kept out of cache_v2 on purpose: core stays free of brpc
# at the source level, so it still builds and tests standalone. (brpc is
# already on the link line through dingofs_common, so this costs no new
# third-party dependency -- only the generated protobuf.)
#
# Its own protoc rule, because the top-level one globs proto/ -- a submodule we
# do not own -- and hard-codes the output path.

set(V2_BRPC_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/transport.proto)
set(V2_BRPC_PB_CC ${CMAKE_CURRENT_BINARY_DIR}/transport.pb.cc)
set(V2_BRPC_PB_H ${CMAKE_CURRENT_BINARY_DIR}/transport.pb.h)

add_custom_command(
    OUTPUT ${V2_BRPC_PB_CC} ${V2_BRPC_PB_H}
    COMMAND ${PROTOC} --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
            --cpp_out=${CMAKE_CURRENT_BINARY_DIR} ${V2_BRPC_PROTO}
    DEPENDS ${V2_BRPC_PROTO}
    COMMENT "Generating transport.pb.{h,cc}"
    VERBATIM)

add_library(cache_v2_brpc
    transport.cc   # the brpc::Server, and the service that receives a request
    call.cc        # one request, from the receiving bthread to its shard
    request.cc     # what a handler sees, once it is on that shard
    client.cc      # the caller's side
    ${V2_BRPC_PB_CC})

target_include_directories(cache_v2_brpc PUBLIC
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROJECT_SOURCE_DIR}/src)
target_compile_features(cache_v2_brpc PRIVATE cxx_std_20)  # coroutine api
# Same tuning as core: this code sits on the same request path.
cache_tune(cache_v2_brpc)
target_link_libraries(cache_v2_brpc PUBLIC cache_v2_net brpc::brpc)
