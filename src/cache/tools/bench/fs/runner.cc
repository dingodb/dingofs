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

#include "cache/tools/bench/fs/runner.h"

#include <butil/time.h>
#include <glog/logging.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "cache/common/slab_buffer.h"
#include "cache/iutil/file_util.h"
#include "cache/iutil/string_util.h"
#include "cache/tools/bench/common/format.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace fs {

namespace {

IOBuffer NewPayload(uint64_t size) {
  char* data = new char[size];
  std::memset(data, 0, size);
  IOBuffer buffer;
  buffer.AppendUserData(data, size, iutil::DeleteBuffer);
  return buffer;
}

void ParallelFor(uint64_t n, uint32_t threads,
                 const std::function<void(uint64_t)>& fn) {
  std::atomic<uint64_t> next{0};
  std::vector<std::thread> pool;
  pool.reserve(threads);
  for (uint32_t t = 0; t < threads; ++t) {
    pool.emplace_back([&]() {
      for (;;) {
        const uint64_t i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= n) break;
        fn(i);
      }
    });
  }
  for (auto& th : pool) th.join();
}

ConsoleReporter::HeaderRows BuildHeader(const Options& o) {
  ConsoleReporter::HeaderRows h;
  h.emplace_back("dir", o.dir);
  h.emplace_back("pattern", RwName(o.rw));
  if (o.rw == Rw::kRandRw) {
    h.emplace_back("rwmixread", std::to_string(o.rwmixread) + "%");
  }
  h.emplace_back("block size", FormatBytes(o.bs));
  h.emplace_back("nrfiles", std::to_string(o.nrfiles));
  h.emplace_back("jobs", std::to_string(o.jobs));
  h.emplace_back("iodepth", std::to_string(o.iodepth));
  if (o.runtime_s > 0) h.emplace_back("runtime", std::to_string(o.runtime_s) + "s");
  if (o.io_size > 0) h.emplace_back("io_size", FormatBytes(o.io_size));
  if (o.warmup_s > 0) h.emplace_back("warmup", std::to_string(o.warmup_s) + "s");
  return h;
}

}  // namespace

Runner::Runner(Options options)
    : options_(options),
      stats_(std::make_unique<Stats>(options_.jobs)),
      reporter_(std::make_unique<ConsoleReporter>(
          "DingoFS cb fs (LocalFileSystem)", BuildHeader(options_),
          options_.report_interval_s, options_.warmup_s, stats_.get())) {}

Runner::~Runner() { Shutdown(); }

std::string Runner::BlockPath(uint64_t key) const {
  return options_.dir + "/blocks/" + std::to_string(key % 256) + "/" +
         std::to_string(key) + ".blk";
}

Status Runner::Init() {
  auto status = InitializeGlobalSlabPool();
  if (!status.ok()) {
    LOG(ERROR) << "Init slab pool failed (need huge pages: 2 x iodepth x 4MiB)";
    return status;
  }
  status = iutil::MkDirs(options_.dir + "/probe");
  if (!status.ok() && !status.IsExist()) {
    return status;
  }

  layout_ = std::make_shared<DiskCacheLayout>(0, options_.dir);
  fs_ = std::make_unique<LocalFileSystem>(layout_);
  DINGOFS_RETURN_NOT_OK(fs_->Start());

  payload_ = NewPayload(options_.bs);
  if (options_.prep && options_.DoesRead()) {
    DINGOFS_RETURN_NOT_OK(PrepBlocks());
  }
  return Status::OK();
}

Status Runner::PrepBlocks() {
  std::cout << "preparing " << options_.nrfiles << " blocks of "
            << FormatBytes(options_.bs) << " ...\n"
            << std::flush;
  std::atomic<bool> failed{false};
  ParallelFor(options_.nrfiles, options_.jobs, [&](uint64_t key) {
    if (failed.load(std::memory_order_relaxed)) return;
    auto status = fs_->WriteFile(BlockPath(key), &payload_);
    if (!status.ok()) {
      LOG(ERROR) << "prep write block " << key << ": " << status.ToString();
      failed.store(true, std::memory_order_relaxed);
    }
  });
  if (failed.load(std::memory_order_relaxed)) {
    return Status::Internal("prep blocks failed");
  }
  return Status::OK();
}

void Runner::RunWorker(uint32_t id) {
  const uint64_t bs = options_.bs;
  const uint64_t nblocks = options_.nrfiles;
  const bool random = options_.IsRandom();
  const bool always_read =
      options_.rw == Rw::kRead || options_.rw == Rw::kRandRead;
  const bool always_write =
      options_.rw == Rw::kWrite || options_.rw == Rw::kRandWrite;

  uint64_t seq_key = id;
  std::mt19937_64 rng(0x9E3779B97F4A7C15ULL ^
                      (static_cast<uint64_t>(id + 1) * 0x100000001B3ULL));

  for (;;) {
    if (stop_.load(std::memory_order_relaxed)) break;
    if (deadline_us_ != 0 && butil::gettimeofday_us() >= deadline_us_) break;
    if (options_.io_size != 0 &&
        bytes_issued_.fetch_add(bs, std::memory_order_relaxed) >=
            options_.io_size) {
      break;
    }

    uint64_t key;
    if (random) {
      key = rng() % nblocks;
    } else {
      key = seq_key % nblocks;
      seq_key += options_.jobs;
    }

    bool for_read = true;
    if (always_write) {
      for_read = false;
    } else if (!always_read) {
      for_read = (rng() % 100) < options_.rwmixread;
    }

    const std::string path = BlockPath(key);
    const uint64_t start_us = butil::gettimeofday_us();
    Status status;
    if (for_read) {
      IOBuffer buffer;
      status = fs_->ReadFile(path, 0, bs, &buffer);
    } else {
      status = fs_->WriteFile(path, &payload_);
    }
    const uint64_t latency_us = butil::gettimeofday_us() - start_us;
    stats_->Record(id, bs, latency_us, status.ok());
  }
}

Status Runner::Run() {
  auto status = Init();
  if (!status.ok()) {
    Shutdown();
    return status;
  }
  status = reporter_->Start();
  if (!status.ok()) {
    Shutdown();
    return status;
  }

  const uint64_t start_us = butil::gettimeofday_us();
  if (options_.runtime_s > 0) {
    deadline_us_ = start_us + (static_cast<uint64_t>(options_.warmup_s +
                                                     options_.runtime_s) *
                              1000000ULL);
  }

  std::vector<std::thread> workers;
  workers.reserve(options_.jobs);
  for (uint32_t i = 0; i < options_.jobs; ++i) {
    workers.emplace_back([this, i]() { RunWorker(i); });
  }

  if (options_.runtime_s > 0) {
    if (options_.warmup_s > 0) {
      ::sleep(options_.warmup_s);
      stats_->Reset();
      reporter_->ResetBaseline();
    }
    ::sleep(options_.runtime_s);
    stop_.store(true, std::memory_order_relaxed);
  }

  for (auto& worker : workers) worker.join();

  reporter_->Stop();
  Shutdown();
  return Status::OK();
}

void Runner::Shutdown() {
  if (fs_ != nullptr) {
    auto status = fs_->Shutdown();
    if (!status.ok()) {
      LOG(ERROR) << "Shutdown LocalFileSystem failed: " << status.ToString();
    }
    fs_.reset();
  }
  if (!options_.keep && !options_.dir.empty()) {
    std::error_code ec;
    std::filesystem::remove_all(options_.dir + "/blocks", ec);
    std::filesystem::remove_all(options_.dir + "/probe", ec);
  }
}

}  // namespace fs
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
