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

add_subdirectory(executor)

add_library(dingofs_auth
    authenticator.cpp
)
target_link_libraries(dingofs_auth
    brpc::brpc
    glog::glog
)

add_library(dingofs_utils
    channel_pool.cpp
    configuration.cpp
    leaky_bucket.cpp
    location_operator.cpp
    stringstatus.cpp
    task_tracker.cpp
    throttle.cpp
    wait_interval.cpp
    bitmap.cpp
    thread_util.cpp
)

target_link_libraries(dingofs_utils
    uuid_static
    brpc::brpc
    glog::glog
)
