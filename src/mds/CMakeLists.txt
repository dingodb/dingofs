# Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

add_subdirectory(codec)
add_subdirectory(dlock)
add_subdirectory(kvstorageclient)
add_subdirectory(idgenerator)
add_subdirectory(leader_election)
add_subdirectory(metaserverclient)
add_subdirectory(topology)
add_subdirectory(schedule)
add_subdirectory(heartbeat)
add_subdirectory(cachegroup)
add_subdirectory(metric)

add_library(mds_lib
    fs_info_wrapper.cpp
    fs_manager.cpp
    fs_storage.cpp
    mds_service.cpp
    mds.cpp
)
target_link_libraries(mds_lib
    PROTO_OBJS
    mds_codec
    mds_dlock
    mds_heartbeat
    mds_metaserverclient
    mds_metric
    mds_schedule
    mds_schedule_service
    mds_topology
    mds_idgenerator
    mds_leader_election
    mds_kvstorage_client
    mds_cachegroup
    dingofs_base_lib
    dingofs_common
    dynamic_vlog
    dingofs_utils
    block_accesser

    dingofs_concurrent
    stub_common
    brpc::brpc
    gflags::gflags
    glog::glog
)

add_executable(dingo-mds main.cpp)
target_link_libraries(dingo-mds
    mds_lib
)