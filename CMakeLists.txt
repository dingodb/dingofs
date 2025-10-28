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

# if compile_commands.json is needed, please enable CMAKE_EXPORT_COMPILE_COMMANDS, of use `bear --append -- make` to do make, it's more recommended to use bear.

cmake_minimum_required(VERSION 3.25 FATAL_ERROR)
project(dingofs C CXX)

option(BUILD_UNIT_TESTS "Build unit test" OFF)
option(USE_TCMALLOC "Use tcmalloc" ON)

set(CMAKE_CXX_STANDARD 17)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug")
endif()
message("CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

message("SYSTEM: ${CMAKE_HOST_SYSTEM_NAME} ${CMAKE_HOST_SYSTEM_PROCESSOR}")

set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")

set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
message("CMAKE_FIND_LIBRARY_SUFFIXES: ${CMAKE_FIND_LIBRARY_SUFFIXES}")

# bin output dir
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(TEST_EXECUTABLE_OUTPUT_PATH ${EXECUTABLE_OUTPUT_PATH}/test)

# third-party install path
if(NOT THIRD_PARTY_INSTALL_PATH)
  if(DEFINED ENV{THIRD_PARTY_INSTALL_PATH})
    set(THIRD_PARTY_INSTALL_PATH $ENV{THIRD_PARTY_INSTALL_PATH})
  else()
    set(THIRD_PARTY_INSTALL_PATH "$ENV{HOME}/.local/dingo-eureka")
  endif()
endif()
message("THIRD_PARTY_INSTALL_PATH:${THIRD_PARTY_INSTALL_PATH}")

# dingosdk install dir
if(NOT DINGOSDK_INSTALL_PATH)
  if(DEFINED ENV{DINGOSDK_INSTALL_PATH})
    set(DINGOSDK_INSTALL_PATH $ENV{DINGOSDK_INSTALL_PATH})
  else()
    set(DINGOSDK_INSTALL_PATH "$ENV{HOME}/.local/dingo-sdk")
  endif()
endif()
message("DINGOSDK_INSTALL_PATH:${DINGOSDK_INSTALL_PATH}")

set(CMAKE_PREFIX_PATH
  ${THIRD_PARTY_INSTALL_PATH}
  ${PROJECT_SOURCE_DIR}/thirdparties/etcdclient
  ${DINGOSDK_INSTALL_PATH}
  ${CMAKE_PREFIX_PATH}
  )
message("CMAKE_PREFIX_PATH:${CMAKE_PREFIX_PATH}")

# link first lookup from THIRD_PARTY_INSTALL_PATH
link_directories(BEFORE ${THIRD_PARTY_INSTALL_PATH}/lib)
link_directories(BEFORE ${PROJECT_SOURCE_DIR}/thirdparties/etcdclient)
link_directories(BEFORE ${DINGOSDK_INSTALL_PATH}/lib)

# include dir
include_directories(${THIRD_PARTY_INSTALL_PATH}/include)
include_directories(${PROJECT_SOURCE_DIR}/thirdparties/etcdclient)
include_directories(${DINGOSDK_INSTALL_PATH}/include)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
message("CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

add_definitions(-DUSE_BTHREAD_MUTEX)
add_definitions(-DSPDLOG_FMT_EXTERNAL)

execute_process(
  COMMAND git describe --always --dirty --tags
  OUTPUT_VARIABLE GIT_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git describe --abbrev=0 --tags --always
  OUTPUT_VARIABLE GIT_TAG_NAME
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log --pretty=format:%an -1
  OUTPUT_VARIABLE GIT_COMMIT_USER
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log --pretty=format:%ae -1
  OUTPUT_VARIABLE GIT_COMMIT_MAIL
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log --pretty=format:%ai -1
  OUTPUT_VARIABLE GIT_COMMIT_TIME
  OUTPUT_STRIP_TRAILING_WHITESPACE)

execute_process(
  COMMAND git log -1 --format=%h
  OUTPUT_VARIABLE GIT_LAST_COMMIT_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(NOT GIT_VERSION)
  set(GIT_VERSION "unknown")
  message(WARNING "Git version is unknown")
endif()

if(NOT GIT_TAG_NAME)
  set(GIT_TAG_NAME "unknown")
  message(WARNING "Git tag name is unknown")
endif()

message(STATUS "Git version: ${GIT_VERSION}")
message(STATUS "Git tag name: ${GIT_TAG_NAME}")
message(STATUS "Git commit user: ${GIT_COMMIT_USER}")
message(STATUS "Git commit mail: ${GIT_COMMIT_MAIL}")
message(STATUS "Git commit time: ${GIT_COMMIT_TIME}")
message(STATUS "Git last commit id: ${GIT_LAST_COMMIT_ID}")

add_definitions(-DGIT_VERSION="${GIT_VERSION}")
add_definitions(-DGIT_TAG_NAME="${GIT_TAG_NAME}")
add_definitions(-DGIT_COMMIT_USER="${GIT_COMMIT_USER}")
add_definitions(-DGIT_COMMIT_MAIL="${GIT_COMMIT_MAIL}")
add_definitions(-DGIT_COMMIT_TIME="${GIT_COMMIT_TIME}")
add_definitions(-DGIT_LAST_COMMIT_ID="${GIT_LAST_COMMIT_ID}")
add_definitions(-DDINGOFS_BUILD_TYPE="${CMAKE_BUILD_TYPE}")

# find dingoEureka
find_package(dingoEureka 1.0 REQUIRED)

find_package(backtrace REQUIRED)
message("Using backtrace ${LIBBACKTRACE_VERSION}, lib:${LIBBACKTRACE_LIBRARY}")

find_package(libunwind REQUIRED)
message("Using libunwind ${LIBUNWIND_VERSION}, lib:${LIBUNWIND_LIBRARY}")


find_package(FUSE3 REQUIRED)
message("Using FUSE3 include_dir:${FUSE3_INCLUDE_DIR}, library:${FUSE3_LIBRARY}")
include_directories(${FUSE3_INCLUDE_DIR})

find_package(LIBMEMCACHED REQUIRED)
message("Using LIBMEMCACHED ${LIBMEMCACHED_VERSION}, lib:${LIBMEMCACHED_LIBRARY}")

# find third-party
set(gflags_DIR ${THIRD_PARTY_INSTALL_PATH}/lib/cmake/gflags)
find_package(gflags REQUIRED)
message("Using gflags ${gflags_VERSION}, include_dir:${gflags_INCLUDE_DIR}")

# openssl
if(NOT OPENSSL_ROOT_DIR)
  set(OPENSSL_ROOT_DIR ${THIRD_PARTY_INSTALL_PATH})
endif()
find_package(OpenSSL REQUIRED)
message("OpenSSL libraries: ${OPENSSL_LIBRARIES}, include dir: ${OPENSSL_INCLUDE_DIR}")

find_package(Snappy)
message("Using Snappy ${Snappy_VERSION}")

find_package(toml11 REQUIRED)
message("Using toml11 ${toml11_VERSION}")

find_package(fmt REQUIRED)
message("Using fmt ${fmt_VERSION}")

set(ZLIB_USE_STATIC_LIBS "ON")
find_package(ZLIB REQUIRED)
message("Using zlib ${ZLIB_VERSION}, include_dir:${ZLIB_INCLUDE_DIR}")

find_package (glog REQUIRED)
# used for glog 0.7.0
add_compile_definitions(GLOG_USE_GLOG_EXPORT)
message("Using glog ${glog_VERSION}")

find_package(GTest CONFIG REQUIRED)
message("Using GTest ${GTest_VERSION}")

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message("Using protobuf ${Protobuf_VERSION}" )

find_package(fiu REQUIRED)
message("Using fiu: ${FIU_LIBRARIES}, include dir: ${FIU_INCLUDE_DIRS}")

find_program(PROTOC protoc REQUIRED)
message("Using protoc ${PROTOC}")

find_package(AWSSDK REQUIRED COMPONENTS s3-crt s3)
message("Using AWSSDK ${AWSSDK_VERSION}, AWSSDK_LINK_LIBRARIES: ${AWSSDK_LINK_LIBRARIES}")

find_package(absl REQUIRED)
message("Using absl ${absl_VERSION}")

find_package(nlohmann_json REQUIRED)
message("Using nlohmann_json ${nlohmann_json_VERSION}")

find_package(spdlog REQUIRED)
message("Using spdlog ${spdlog_VERSION}")

find_package(opentelemetry-cpp CONFIG REQUIRED)
message("Using opentelemetry-cpp ${opentelemetry-cpp_VERSION}")

# depends by rocksdb
find_package(uring REQUIRED)
message("Using uring ${uring_VERSION}")

find_package(hashkit REQUIRED)
message("Using hashkit: ${hashkit_LIBRARIES}")

find_package(jsoncpp REQUIRED)
message("Using jsoncpp ${jsoncpp_VERSION}")

find_package(leveldb REQUIRED)
message("Using leveldb ${leveldb_VERSION}")

find_package(brpc REQUIRED)
find_package(braft REQUIRED)
find_package(RocksDB REQUIRED)
message("Using RocksDB ${RocksDB_VERSION}")

find_package(rados REQUIRED)
message("Using rados: ${RADOS_LIBRARIES}")

find_package(dingosdk REQUIRED)
message(STATUS "Found dingosdk: ${dingosdk_VERSION}")

# ------------------------ related to source code ------------------------

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

set(PROTO_OUTPUT_DIR ${CMAKE_BINARY_DIR}/protos)

if(EXISTS ${PROTO_OUTPUT_DIR} AND IS_DIRECTORY ${PROTO_OUTPUT_DIR})
  message(STATUS "PROTO_OUTPUT_DIR:${PROTO_OUTPUT_DIR}")
else()
  file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})
  message(STATUS "Create PROTO_OUTPUT_DIR:${PROTO_OUTPUT_DIR}")
endif()

include_directories(${PROTO_OUTPUT_DIR})

set(PROTOS_DIR ${PROJECT_SOURCE_DIR}/proto)
message("PROTOS_DIR:${PROTOS_DIR}")
file(GLOB_RECURSE MSG_PROTOS ${PROTOS_DIR}/*.proto)

set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(msg ${MSG_PROTOS})
        get_filename_component(FIL_WE ${msg} NAME_WE)

        message(STATUS "proto file: ${msg}")

        set(FILE_PREFIX_NAME "${PROTO_OUTPUT_DIR}/dingofs/${FIL_WE}")

        set(PROTO_PB_SRC "${FILE_PREFIX_NAME}.pb.cc")
        message(STATUS "PROTO_PB_SRC: ${PROTO_PB_SRC}")
        list(APPEND PROTO_SRCS "${PROTO_PB_SRC}")

        set(PROTO_PB_HDR "${FILE_PREFIX_NAME}.pb.h")
        message(STATUS "PROTO_PB_HDR: ${PROTO_PB_HDR}")
        list(APPEND PROTO_HDRS "${PROTO_PB_HDR}")

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
  add_subdirectory(test/unit)
endif()



