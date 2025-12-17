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

add_subdirectory(files)
add_subdirectory(s3)
add_subdirectory(rados)

add_library(block_accesser
    block_accesser.cc
    block_access_log.cc
)

target_link_libraries(block_accesser
    file_accesser
    s3_accesser
    rados_accesser
    dingofs_common
)
