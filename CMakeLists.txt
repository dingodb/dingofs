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

set(CMAKE_PREFIX_PATH
  ${THIRD_PARTY_INSTALL_PATH}
  ${PROJECT_SOURCE_DIR}/thirdparties/etcdclient
  ${CMAKE_PREFIX_PATH}
  )
message("CMAKE_PREFIX_PATH:${CMAKE_PREFIX_PATH}")

# link first lookup from THIRD_PARTY_INSTALL_PATH
link_directories(BEFORE ${THIRD_PARTY_INSTALL_PATH}/lib)
link_directories(BEFORE ${PROJECT_SOURCE_DIR}/thirdparties/etcdclient)

# include dir
include_directories(${THIRD_PARTY_INSTALL_PATH}/include)
include_directories(${PROJECT_SOURCE_DIR}/thirdparties/etcdclient)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
add_definitions(-DUSE_BTHREAD_MUTEX)

find_package(FUSE3 REQUIRED)
message("Using FUSE3 ${FUSE3_VERSION}, include_dir:${FUSE3_INCLUDE_DIR}")
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

find_program(PROTOC protoc REQUIRED)
message("Using protoc ${PROTOC}")

find_package(AWSSDK COMPONENTS s3)
message("Using AWSSDK ${AWSSDK_VERSION}, lib:${AWSSDK_LIBRARIES}")

find_package(absl REQUIRED)
message("Using absl ${absl_VERSION}")

find_package(nlohmann_json REQUIRED)
message("Using nlohmann_json ${nlohmann_json_VERSION}")

find_package(opentelemetry-cpp CONFIG REQUIRED)
message("Using opentelemetry-cpp ${opentelemetry-cpp_VERSION}")

# depends by rocksdb
find_package(uring)
message("Using uring ${uring_VERSION}")

find_package(jsoncpp REQUIRED)
message("Using jsoncpp ${jsoncpp_VERSION}")

find_package(brpc REQUIRED)
find_package(braft REQUIRED)
find_package(RocksDB REQUIRED)

add_subdirectory(dingofs)