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

add_subdirectory(buffer)
add_subdirectory(vfs)
add_subdirectory(fuse)

add_executable(dingo-client main.cc)
# ibverbs::ibverbs (shared libibverbs.so) MUST come first. fuse_client_lib's
# transitive storage stack links static librados/librdmacm, which cross-pull
# libibverbs.a; if that static archive is reached before the .so, the linker
# binds DingoFS's own ibv_* calls to the static copy. Then at runtime the mlx5
# provider (dlopen'd via libibverbs.so) registers into a different libibverbs
# instance, so device open fails with "no userspace device-specific driver
# found for mlx5_0". Listing the .so first makes ibv_* bind dynamically; the
# --exclude-libs is belt-and-suspenders so any pulled archive symbols stay
# hidden and can never shadow the .so.
target_link_libraries(dingo-client
    ibverbs::ibverbs
    fuse_client_lib
    glog::glog
)
target_link_options(dingo-client PRIVATE
    "-Wl,--exclude-libs,libibverbs.a"
)

if(DINGOFS_BUILD_SDK)
    include(${CMAKE_CURRENT_LIST_DIR}/cmake/bundle.cmake)
    include(${CMAKE_CURRENT_LIST_DIR}/cmake/install.cmake)
endif()
