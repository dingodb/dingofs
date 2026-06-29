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

# cb - one binary, one subcommand per cache layer:
#   cb aio | fs | store | client | fsop
# Each subcommand lives in its own directory and namespace; common/ holds the
# shared flag parser, formatter, histogram, stats and reporter.
add_executable(cb
    main.cc

    common/flags.cc
    common/format.cc
    common/histogram.cc
    common/stats.cc
    common/reporter.cc

    aio/options.cc
    aio/runner.cc
    aio/command.cc

    fs/options.cc
    fs/runner.cc
    fs/command.cc

    store/options.cc
    store/runner.cc
    store/command.cc

    client/options.cc
    client/key_generator.cc
    client/operation.cc
    client/runner.cc
    client/command.cc

    fsop/options.cc
    fsop/runner.cc
    fsop/command.cc
)

target_link_libraries(cb
    cache_tier
    cache_local
    cache_common
    cache_iutil
    block_accesser
    fake_accesser

    dingofs_common
    dingofs_options
    dingofs_executor
    dingofs_write_mempool

    uring::uring
    brpc::brpc
    fmt::fmt
    gflags::gflags
    glog::glog
    pthread
)
