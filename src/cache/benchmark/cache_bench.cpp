/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-05-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/cache_bench.h"

#include <absl/strings/str_format.h>
#include <butil/iobuf.h>
#include <butil/time.h>
#include <fcntl.h>
#include <gflags/gflags.h>

#include <iostream>
#include <memory>

#include "base/math/math.h"
#include "base/string/string.h"
#include "cache/blockcache/cache_store.h"
#include "cache/remotecache/remote_block_cache.h"
#include "cache/utils/local_filesystem.h"
#include "cache/utils/posix.h"

DEFINE_string(conf, "", "configure file path");
DEFINE_string(block_key, "", "block key");
DEFINE_uint32(block_size, 4194304, "block total size");
DEFINE_uint32(block_offset, 0, "block offset for range request");
DEFINE_uint32(block_length, 4194304, "block length for range request");
DEFINE_string(dump_file, "", "dump block content to specified file");

namespace dingofs {
namespace cache {
namespace benchmark {

using base::math::kMiB;
using dingofs::base::string::StrFormat;
using dingofs::cache::blockcache::BlockKey;
using dingofs::cache::remotecache::RemoteBlockCacheImpl;
using dingofs::cache::utils::Posix;

CacheBenchImpl::CacheBenchImpl(RemoteBlockCacheOption option)
    : option_(option),
      remote_block_cache_(std::make_unique<RemoteBlockCacheImpl>(option)) {}

Status CacheBenchImpl::Init() { return remote_block_cache_->Init(); }

Status CacheBenchImpl::RangeBlock(const BlockKey& block_key, size_t block_size,
                                  off_t offset, size_t length) {
  butil::Timer timer;
  butil::IOBuf buffer;

  timer.start();
  Status status = remote_block_cache_->Range(block_key, block_size, offset,
                                             length, &buffer);
  timer.stop();

  if (!status.ok()) {
    std::cerr << "Range(" << block_key.Filename() << "," << block_size << ","
              << offset << "," << length << ") failed: " << status.ToString();
    return status;
  }

  if (!FLAGS_dump_file.empty()) {
    DumpBlock(FLAGS_dump_file, &buffer, length);
  }

  std::cout << StrFormat("Range(%s,%d,%d,%d): OK <%.6lf>", block_key.Filename(),
                         block_size, offset, length, timer.u_elapsed() / 1e6)
            << std::endl;
  return status;
}

void CacheBenchImpl::DumpBlock(const std::string& filepath,
                               butil::IOBuf* buffer, size_t length) {
  int fd;
  auto status = Posix::Create(filepath, &fd, false);
  if (!status.ok()) {
    std::cerr << "Open file(path=" << filepath
              << ") failed: " << status.ToString();
    return;
  }

  while (length > 0) {
    static size_t size = std::min(1 * base::math::kMiB, length);
    ssize_t n = buffer->cut_into_file_descriptor(fd, size);
    if (n < 0) {
      std::cerr << "Dump block to file failed: filepath = " << FLAGS_dump_file
                << std::endl;
    }
    length -= n;
  }
}

void CacheBenchImpl::Run() {
  BlockKey block_key;
  if (!block_key.ParseFilename(FLAGS_block_key)) {
    std::cerr << "Invalid block key " << FLAGS_block_key;
    return;
  }
  RangeBlock(block_key, FLAGS_block_size, FLAGS_block_offset,
             FLAGS_block_length);
}

}  // namespace benchmark
}  // namespace cache
}  // namespace dingofs

using dingofs::cache::RemoteBlockCacheOption;
using dingofs::cache::benchmark::CacheBenchImpl;

void RunBench() {}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);
  if (FLAGS_conf.empty()) {
    std::cerr << "Please specify config path." << std::endl;
    return -1;
  }

  RemoteBlockCacheOption option;
  if (!option.Parse(FLAGS_conf)) {
    std::cerr << "Parse configure file failed, conf_path = " << FLAGS_conf
              << std::endl;
    return -1;
  }

  CacheBenchImpl bench(option);
  auto status = bench.Init();
  if (!status.ok()) {
    std::cerr << "Init cache bench failed: " << status.ToString();
    return -1;
  }
  bench.Run();
}
