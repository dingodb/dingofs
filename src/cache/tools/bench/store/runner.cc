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

#include "cache/tools/bench/store/runner.h"

#include <butil/time.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "cache/common/slab_buffer.h"
#include "cache/common/storage_client.h"
#include "cache/iutil/string_util.h"
#include "cache/local/cache_store.h"
#include "cache/local/disk_cache.h"
#include "cache/local/local_block_cache.h"
#include "cache/local/mem_cache.h"
#include "cache/tools/bench/common/format.h"
#include "common/block/block_key.h"
#include "common/blockaccess/block_accesser.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace store {

namespace {

constexpr const char* kBenchUuid = "cb-store";

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

void NoopUploader(BlockHandle /*handle*/, size_t /*length*/,
                  BlockAttr /*attr*/) {}

// diskcache / mem: a CacheStore. Write/Fill = Cache(), Read = Load().
class CacheStoreTarget final : public BenchTarget {
 public:
  explicit CacheStoreTarget(CacheStoreSPtr store) : store_(std::move(store)) {}
  Status Start() override { return store_->Start(NoopUploader); }
  void Shutdown() override {
    if (store_ != nullptr) store_->Shutdown();
  }
  Status Write(const BlockHandle& h, IOBuffer p) override {
    return store_->Cache(h, std::move(p));
  }
  Status Read(const BlockHandle& h, off_t off, size_t len,
              IOBuffer* buf) override {
    return store_->Load(h, off, len, buf);
  }
  Status Fill(const BlockHandle& h, IOBuffer p) override {
    return store_->Cache(h, std::move(p));
  }

 private:
  CacheStoreSPtr store_;
};

// blockcache: LocalBlockCache over an in-memory fake storage backend. Write =
// Cache() (idempotent, so random patterns and re-writes are safe; the writeback
// Put() Stage path CHECK-fails on re-staging a cached/promoted block). Read =
// Range(retrieve_storage=false).
class BlockCacheTarget final : public BenchTarget {
 public:
  explicit BlockCacheTarget(Options options) : options_(std::move(options)) {}

  Status Start() override {
    google::SetCommandLineOption("use_fake_block_access", "true");
    blockaccess::BlockAccessOptions access_options;
    block_accesser_ = blockaccess::NewBlockAccesser(access_options);
    DINGOFS_RETURN_NOT_OK(block_accesser_->Init());
    storage_client_ = std::make_unique<StorageClient>(block_accesser_.get());
    DINGOFS_RETURN_NOT_OK(storage_client_->Start());
    block_cache_ = std::make_unique<LocalBlockCache>(storage_client_.get());
    return block_cache_->Start();
  }
  void Shutdown() override {
    if (block_cache_ != nullptr) block_cache_->Shutdown();
    if (storage_client_ != nullptr) storage_client_->Shutdown();
    if (block_accesser_ != nullptr) block_accesser_->Destroy();
  }
  Status Write(const BlockHandle& h, IOBuffer p) override {
    return block_cache_->Cache(h, std::move(p));
  }
  Status Read(const BlockHandle& h, off_t off, size_t len,
              IOBuffer* buf) override {
    RangeOption option;
    option.retrieve_storage = false;
    option.block_whole_length = options_.bs;
    return block_cache_->Range(h, off, len, buf, option);
  }
  Status Fill(const BlockHandle& h, IOBuffer p) override {
    return block_cache_->Cache(h, std::move(p));
  }

 private:
  Options options_;
  blockaccess::BlockAccesserUPtr block_accesser_;
  std::unique_ptr<StorageClient> storage_client_;
  std::unique_ptr<LocalBlockCache> block_cache_;
};

ConsoleReporter::HeaderRows BuildHeader(const Options& o) {
  ConsoleReporter::HeaderRows h;
  const char* read_op = o.layer == Layer::kBlockCache ? "Range" : "Load";
  h.emplace_back("layer", LayerName(o.layer));
  h.emplace_back("ops", std::string("write=Cache  read=") + read_op);
  if (o.NeedsDisk()) h.emplace_back("dir", o.dir);
  h.emplace_back("pattern", RwName(o.rw));
  if (o.rw == Rw::kRandRw) {
    h.emplace_back("rwmixread", std::to_string(o.rwmixread) + "%");
  }
  h.emplace_back("block size", FormatBytes(o.bs));
  h.emplace_back("nrfiles", std::to_string(o.nrfiles));
  h.emplace_back("jobs", std::to_string(o.jobs));
  if (o.NeedsDisk()) h.emplace_back("iodepth", std::to_string(o.iodepth));
  h.emplace_back(
      "store size",
      FormatBytes(static_cast<uint64_t>(o.store_size_mb) * 1024 * 1024));
  if (o.runtime_s > 0) h.emplace_back("runtime", std::to_string(o.runtime_s) + "s");
  if (o.io_size > 0) h.emplace_back("io_size", FormatBytes(o.io_size));
  if (o.warmup_s > 0) h.emplace_back("warmup", std::to_string(o.warmup_s) + "s");
  return h;
}

}  // namespace

BenchTargetUPtr NewTarget(const Options& options) {
  switch (options.layer) {
    case Layer::kDiskCache: {
      DiskCacheOption opt;
      opt.cache_index = 0;
      opt.cache_store = "disk";
      opt.cache_dir = options.dir;
      opt.cache_size_mb = options.store_size_mb;
      return std::make_unique<CacheStoreTarget>(std::make_shared<DiskCache>(opt));
    }
    case Layer::kMem: {
      MemCacheOption opt;
      opt.cache_size_mb = options.store_size_mb;
      return std::make_unique<CacheStoreTarget>(std::make_shared<MemCache>(opt));
    }
    case Layer::kBlockCache:
      return std::make_unique<BlockCacheTarget>(options);
  }
  return nullptr;
}

Runner::Runner(Options options)
    : options_(options),
      stats_(std::make_unique<Stats>(options_.jobs)),
      reporter_(std::make_unique<ConsoleReporter>(
          "DingoFS cb store", BuildHeader(options_), options_.report_interval_s,
          options_.warmup_s, stats_.get())) {}

Runner::~Runner() { Shutdown(); }

BlockHandle Runner::MakeHandle(uint64_t key) const {
  BlockKey block_key(key + 1, 0, static_cast<uint32_t>(options_.bs));
  return BlockHandle(1, block_key);
}

Status Runner::Init() {
  if (options_.NeedsDisk()) {
    auto status = InitializeGlobalSlabPool();
    if (!status.ok()) {
      LOG(ERROR) << "Init slab pool failed (need huge pages: 2 x iodepth x 4MiB)";
      return status;
    }
    std::error_code ec;
    std::filesystem::create_directories(options_.dir, ec);
  }

  FLAGS_cache_store = options_.layer == Layer::kMem ? "memory" : "disk";
  FLAGS_cache_dir = options_.dir;
  FLAGS_cache_size_mb = options_.store_size_mb;
  FLAGS_enable_stage = true;
  FLAGS_enable_cache = true;
  google::SetCommandLineOption("cache_dir_uuid", kBenchUuid);

  target_ = NewTarget(options_);
  if (target_ == nullptr) return Status::Internal("unknown layer");
  DINGOFS_RETURN_NOT_OK(target_->Start());

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
    auto status = target_->Fill(MakeHandle(key), payload_);
    if (!status.ok()) {
      LOG(ERROR) << "prep fill block " << key << ": " << status.ToString();
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

    const BlockHandle handle = MakeHandle(key);
    const uint64_t start_us = butil::gettimeofday_us();
    Status status;
    if (for_read) {
      IOBuffer buffer;
      status = target_->Read(handle, 0, bs, &buffer);
    } else {
      status = target_->Write(handle, payload_);
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
  if (target_ != nullptr) {
    target_->Shutdown();
    target_.reset();
  }
  if (!options_.keep && options_.NeedsDisk() && !options_.dir.empty()) {
    std::error_code ec;
    if (options_.layer == Layer::kBlockCache) {
      std::filesystem::remove_all(options_.dir + "/" + kBenchUuid, ec);
    } else {
      for (const char* sub : {"cache", "stage", "probe", ".lock", ".detect"}) {
        std::filesystem::remove_all(options_.dir + "/" + sub, ec);
      }
    }
  }
}

}  // namespace store
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
