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

add_executable(rdma_server rdma_server.cc)
target_link_libraries(rdma_server
    cache_infiniband
)

add_executable(rdma_client rdma_client.cc)
target_link_libraries(rdma_client
    cache_infiniband
)

# brpc/TCP counterparts for latency comparison against the RDMA path.
add_executable(brpc_server brpc_server.cc)
target_link_libraries(brpc_server
    cache_infiniband
)

add_executable(brpc_client brpc_client.cc)
target_link_libraries(brpc_client
    cache_infiniband
)

foreach(RDMA_START_SCRIPT start_rdma_client.sh start_rdma_server.sh)
    file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/${RDMA_START_SCRIPT}
        DESTINATION ${CMAKE_BINARY_DIR}/bin
        FILE_PERMISSIONS
            OWNER_READ OWNER_WRITE OWNER_EXECUTE
            GROUP_READ GROUP_EXECUTE
            WORLD_READ WORLD_EXECUTE
    )
endforeach()
