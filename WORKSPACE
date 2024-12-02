#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

workspace(name = "curve")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# skylib
http_archive(
    name = "bazel_skylib",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
    ],
    sha256 = "af87959afe497dc8dfd4c6cb66e1279cb98ccc84284619ebfec27d9c09a903de",
)
load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
bazel_skylib_workspace()

# C++ rules for Bazel.
http_archive(
    name = "rules_cc",
    urls = ["https://github.com/bazelbuild/rules_cc/archive/9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip"],
    strip_prefix = "rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5",
    sha256 = "954b7a3efc8752da957ae193a13b9133da227bdacf5ceb111f2e11264f7e8c95",
)

# Bazel platform rules.
http_archive(
    name = "platforms",
    sha256 = "b601beaf841244de5c5a50d2b2eddd34839788000fa1be4260ce6603ca0d8eb7",
    strip_prefix = "platforms-98939346da932eef0b54cf808622f5bb0928f00b",
    urls = ["https://github.com/bazelbuild/platforms/archive/98939346da932eef0b54cf808622f5bb0928f00b.zip"],
)

# abseil-cpp
http_archive(
  name = "com_google_absl",
  urls = ["https://github.com/abseil/abseil-cpp/archive/refs/tags/20240116.2.tar.gz"],
  strip_prefix = "abseil-cpp-20240116.2",
  sha256 = "733726b8c3a6d39a4120d7e45ea8b41a434cdacde401cba500f14236c49b39dc",
)

new_local_repository(
    name = "etcdclient",
    build_file = "//:thirdparties/etcdclient.BUILD",
    path = "thirdparties/etcdclient",
)

new_local_repository(
    name = "braft",
    path = "third-party/installed",
    build_file = "//bazel:braft.BUILD",
)

new_local_repository(
    name = "brpc",
    path = "third-party/installed",
    build_file = "//bazel:brpc.BUILD",
)

new_local_repository(
    name = "zlib",
    path = "third-party/installed",
    build_file = "//bazel:zlib.BUILD",
)

new_local_repository(
    name = "protobuf",
    path = "third-party/installed",
    build_file = "//bazel:protobuf.BUILD",
)

new_local_repository(
    name = "gtest",
    path = "third-party/installed",
    build_file = "//bazel:gtest.BUILD",
)

new_local_repository(
    name = "gflags",
    path = "third-party/installed",
    build_file = "//bazel:gflags.BUILD",
)

new_local_repository(
    name = "glog",
    path = "third-party/installed",
    build_file = "//bazel:glog.BUILD",
)

new_local_repository(
    name = "leveldb",
    path = "third-party/installed",
    build_file = "//bazel:leveldb.BUILD",
)

new_local_repository(
    name = "snappy",
    path = "third-party/installed",
    build_file = "//bazel:snappy.BUILD",
)

new_local_repository(
    name = "crc32c",
    path = "third-party/installed",
    build_file = "//bazel:crc32c.BUILD",
)

new_local_repository(
    name = "openssl",
    path = "third-party/installed",
    build_file = "//bazel:openssl.BUILD",
)

new_local_repository(
    name = "jsoncpp",
    path = "third-party/installed",
    build_file = "//bazel:jsoncpp.BUILD",
)

new_local_repository(
    name = "libmemcached",
    path = "third-party/installed",
    build_file = "//bazel:memcache.BUILD",
)

new_local_repository(
    name = "libfiu",
    path = "third-party/installed",
    build_file = "//bazel:libfiu.BUILD",
)

new_local_repository(
    name = "aws",
    path = "third-party/installed",
    build_file = "//bazel:aws.BUILD",
)

new_local_repository(
    name = "fmt",
    path = "third-party/installed",
    build_file = "//bazel:fmt.BUILD",
)

new_local_repository(
    name = "spdlog",
    path = "third-party/installed",
    build_file = "//bazel:spdlog.BUILD",
)

new_local_repository(
    name = "incbin",
    path = "third-party/installed",
    build_file = "//bazel:incbin.BUILD",
)

new_local_repository(
    name = "rocksdb",
    path = "third-party/installed",
    build_file = "//bazel:rocksdb.BUILD",
)

new_local_repository(
    name = "curl",
    path = "third-party/installed",
    build_file = "//bazel:curl.BUILD",
)

new_local_repository(
    name = "uuid",
    path = "third-party/installed",
    build_file = "//bazel:uuid.BUILD",
)

new_local_repository(
    name = "opentelemetry",
    path = "third-party/installed",
    build_file = "//bazel:opentelemetry.BUILD",
)

new_local_repository(
    name = "third-party-headers",
    path = "third-party/installed/include",
    build_file = "//bazel:headers.BUILD",
)

new_local_repository(
    name = "proto_compiler",
    path = "third-party/protobuf/src/google/protobuf/compiler",
    build_file = "//bazel:protoc.BUILD",
)

# Hedron's Compile Commands Extractor for Bazel
# https://github.com/hedronvision/bazel-compile-commands-extractor
http_archive(
    name = "hedron_compile_commands",

    # Replace the commit hash in both places (below) with the latest, rather than using the stale one here.
    # Even better, set up Renovate and let it do the work for you (see "Suggestion: Updates" in the README).
    urls = [
        "https://github.com/hedronvision/bazel-compile-commands-extractor/archive/3dddf205a1f5cde20faf2444c1757abe0564ff4c.tar.gz",
    ],
    strip_prefix = "bazel-compile-commands-extractor-3dddf205a1f5cde20faf2444c1757abe0564ff4c",
    sha256 = "3cd0e49f0f4a6d406c1d74b53b7616f5e24f5fd319eafc1bf8eee6e14124d115",
)
load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")
hedron_compile_commands_setup()
