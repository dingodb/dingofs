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

if(WITH_CLIENT_BLOCKCACHE)
    add_library(test_vfs_blockstore
        test_block_store_v2_util.cc
    )
    target_compile_features(test_vfs_blockstore PRIVATE cxx_std_20)
    target_link_libraries(test_vfs_blockstore
        vfs_block_store
        ${TEST_DEPS_WITHOUT_MAIN}
    )
endif()
