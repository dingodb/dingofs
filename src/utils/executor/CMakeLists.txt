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

add_library(dingofs_executor 
    bthread/bthread_executor.cc
    bthread/bthread_pool.cc
    thread/executor_impl.cc
    thread/thread_pool_impl.cc
    timer/timer_impl.cc
)
    
target_link_libraries(dingofs_executor
    brpc::brpc
    glog::glog
    gflags::gflags
)
