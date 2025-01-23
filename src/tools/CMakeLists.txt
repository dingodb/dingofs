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


# CurveFS Tools Library
file(GLOB DINGOFS_TOOLS_LIB_SRCS
    "*.cpp"
    "create/*.cpp"
    "version/*.cpp"
    "usage/*.cpp"
    "status/*.cpp"
    "query/*.cpp"
    "list/*.cpp"
    "check/*.cpp"
    "copyset/*.cpp"
    "delete/*.cpp"
    "umount/*.cpp"
)


add_library(tools_lib ${DINGOFS_TOOLS_LIB_SRCS})
target_link_libraries(tools_lib
    PROTO_OBJS
    dingofs_common
    dingofs_utils
    jsoncpp
    brpc::brpc
    protobuf::libprotobuf
    absl::cleanup
    absl::strings
    dl
)

add_executable(dingo-tool 
    dingofs_tool_main.cpp)

# use target_link_libraries to link the executable with the library
# the library will be linked to the executable
# i need to link both debug and release libraries,help me fix this
target_link_libraries(dingo-tool
    tools_lib
    gflags::gflags 
)