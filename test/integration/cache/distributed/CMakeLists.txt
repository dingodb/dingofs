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

# Distributed (remote cache node) suite. Built as an object library and linked
# into the single integration_test_cache binary (see ../CMakeLists.txt).
file(GLOB INTEGRATION_CACHE_DISTRIBUTED_SRCS CONFIGURE_DEPENDS "*.cc")

add_library(integration_cache_distributed_tests OBJECT
    ${INTEGRATION_CACHE_DISTRIBUTED_SRCS}
)

target_link_libraries(integration_cache_distributed_tests
    integration_cache_deploy
    ${TEST_INTEGRATION_DEPS_WITHOUT_MAIN}
)
