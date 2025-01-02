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



include_directories(${CMAKE_BINARY_DIR}/protos)

set(PROTO_OUTPUT_DIR ${CMAKE_BINARY_DIR}/protos/dingofs/proto)
add_custom_target(create_proto_output_dir ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory ${PROTO_OUTPUT_DIR}
)
message("PROTO_OUTPUT_DIR:${PROTO_OUTPUT_DIR}")

set(PROTOS_DIR ${PROJECT_SOURCE_DIR}/proto)
message("PROTOS_DIR:${PROTOS_DIR}")
file(GLOB_RECURSE MSG_PROTOS ${PROTOS_DIR}/*.proto)

set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(msg ${MSG_PROTOS})
        get_filename_component(FIL_WE ${msg} NAME_WE)

        message(STATUS "proto file: ${msg}")

        set(FILE_PREFIX_NAME "${PROTO_OUTPUT_DIR}/${FIL_WE}")
        message(STATUS "FILE_PREFIX_NAME: ${FILE_PREFIX_NAME}")
        list(APPEND PROTO_SRCS "${FILE_PREFIX_NAME}.pb.cc")
        list(APPEND PROTO_HDRS "${FILE_PREFIX_NAME}.pb.h")

        add_custom_command(
          OUTPUT "${FILE_PREFIX_NAME}.pb.cc"
             "${FILE_PREFIX_NAME}.pb.h"
          COMMAND  ${PROTOC}
          ARGS --cpp_out  ${PROTO_OUTPUT_DIR}
          ARGS --proto_path=${PROTOS_DIR}
            ${msg}
          DEPENDS ${msg}
          COMMENT "Running C++ protocol buffer compiler on ${msg}"
          VERBATIM
        )
endforeach()

set_source_files_properties(
  ${PROTO_SRCS} 
  ${PROTO_HDRS} 
  PROPERTIES GENERATED TRUE)

set(PROTO_OBJS_SRC
    ${PROTO_SRCS} 
    ${PROTO_HDRS} 
)

add_library(PROTO_OBJS OBJECT 
    ${PROTO_OBJS_SRC} 
)

set_target_properties(PROTO_OBJS
  PROPERTIES
  POSITION_INDEPENDENT_CODE ON
)

add_subdirectory(src)

if(BUILD_UNIT_TESTS)
  message(STATUS "Build unit test")
  add_subdirectory(test)
endif()