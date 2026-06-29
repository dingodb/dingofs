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

#include "cache/tools/bench/client/runner.h"

#include <bthread/bthread.h>
#include <butil/time.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <utility>

#include "cache/tier/tier_block_cache.h"
#include "cache/tools/bench/common/format.h"
#include "common/blockaccess/prefix_block_accesser.h"
#include "common/config_mapper.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace cache {
namespace bench {
namespace client {

namespace {
ConsoleReporter::HeaderRows BuildHeader(const Options& o) {
  ConsoleReporter::HeaderRows h;
  h.emplace_back("operation", OperationName(o.operation));
  h.emplace_back("mds_addrs", o.mds_addrs);
  h.emplace_back("fsid", std::to_string(o.fs_id));
  h.emplace_back("threads", std::to_string(o.threads));
  h.emplace_back("target qps", o.qps > 0
                                   ? std::to_string(static_cast<uint64_t>(o.qps))
                                   : "unlimited");
  h.emplace_back("max inflight", o.max_inflight == 0
                                     ? "unbounded"
                                     : std::to_string(o.max_inflight));
  h.emplace_back("key dist", KeyDistName(o.key_dist));
  h.emplace_back("keyspace", std::to_string(o.keyspace));
  if (o.key_dist == KeyDist::kZipf) {
    std::ostringstream theta;
    theta << std::fixed << std::setprecision(2) << o.zipf_theta;
    h.emplace_back("zipf theta", theta.str());
  }
  h.emplace_back("block size", FormatBytes(o.block_size));
  if (o.duration_s > 0) h.emplace_back("duration", std::to_string(o.duration_s) + "s");
  if (o.ops > 0) h.emplace_back("ops", std::to_string(o.ops));
  if (o.warmup_s > 0) h.emplace_back("warmup", std::to_string(o.warmup_s) + "s");
  if (o.operation == OperationType::kPut) {
    h.emplace_back("writeback", o.writeback ? "yes" : "no");
  } else {
    h.emplace_back("range", FormatBytes(o.range_offset) + " + " +
                                FormatBytes(o.range_length));
    h.emplace_back("retrieve storage", o.retrieve_storage ? "yes" : "no");
  }
  return h;
}
}  // namespace

Runner::Runner(Options options)
    : options_(options),
      stats_(std::make_unique<Stats>(options_.threads)),
      mds_client_(std::make_shared<MDSClientImpl>()),
      reporter_(std::make_unique<ConsoleReporter>(
          "DingoFS cb client (TierBlockCache, open-loop)", BuildHeader(options_),
          options_.report_interval_s, options_.warmup_s, stats_.get())) {
  workers_.reserve(options_.threads);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    workers_.push_back(std::make_unique<Worker>());
  }
}

Runner::~Runner() { Shutdown(); }

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

  const double base_us = options_.qps > 0 ? 1e6 / options_.qps : 0.0;
  schedule_interval_us_ = base_us * options_.threads;
  inflight_per_worker_ = options_.max_inflight == 0
                             ? 0
                             : std::max(1U, options_.max_inflight / options_.threads);

  start_us_ = butil::gettimeofday_us();
  if (options_.duration_s > 0) {
    deadline_us_ = start_us_ + (static_cast<uint64_t>(options_.warmup_s +
                                                      options_.duration_s) *
                               1000000ULL);
  }

  std::vector<bthread_t> tids(options_.threads, 0);
  std::vector<GenArg> args(options_.threads);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    workers_[i]->rng.seed(0x9E3779B97F4A7C15ULL ^
                          (static_cast<uint64_t>(i) * 0x100000001B3ULL));
    workers_[i]->next_schedule_us =
        start_us_ + static_cast<uint64_t>(base_us * i);
    args[i] = GenArg{this, i};
    if (bthread_start_background(&tids[i], nullptr, &Runner::GenEntry,
                                 &args[i]) != 0) {
      tids[i] = 0;
      Generate(i);
    }
  }

  if (options_.warmup_s > 0) {
    ::sleep(options_.warmup_s);
    stats_->Reset();
    reporter_->ResetBaseline();
  }

  for (uint32_t i = 0; i < options_.threads; ++i) {
    if (tids[i] != 0) bthread_join(tids[i], nullptr);
  }

  reporter_->Stop();
  Shutdown();
  return Status::OK();
}

Status Runner::Init() {
  DINGOFS_RETURN_NOT_OK(InitMdsClient());
  DINGOFS_RETURN_NOT_OK(InitBlockAccesser());
  DINGOFS_RETURN_NOT_OK(InitBlockCache());
  operation_ = NewOperation(block_cache_, options_);
  key_generator_ = std::make_unique<KeyGenerator>(
      options_.key_dist, options_.keyspace, options_.zipf_theta,
      static_cast<uint32_t>(options_.block_size));
  return Status::OK();
}

Status Runner::InitMdsClient() { return mds_client_->Start(); }

Status Runner::InitBlockAccesser() {
  pb::mds::FsInfo fs_info;
  auto status = mds_client_->GetFSInfo(options_.fs_id, &fs_info);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get FsInfo for fsid=" << options_.fs_id;
    return status;
  }
  blockaccess::BlockAccessOptions block_access_options;
  FillBlockAccessOption(fs_info, &block_access_options);
  block_accesser_ = blockaccess::NewPrefixBlockAccesser(fs_info.fs_name(),
                                                        block_access_options);
  status = block_accesser_->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init BlockAccesser for fsid=" << options_.fs_id;
  }
  return status;
}

Status Runner::InitBlockCache() {
  block_cache_ = std::make_shared<TierBlockCache>(block_accesser_.get());
  auto status = block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
  }
  return status;
}

void* Runner::GenEntry(void* arg) {
  auto* gen_arg = static_cast<GenArg*>(arg);
  gen_arg->self->Generate(gen_arg->id);
  return nullptr;
}

void Runner::Generate(uint32_t id) {
  auto& worker = *workers_[id];
  for (;;) {
    const uint64_t now = butil::gettimeofday_us();
    if (deadline_us_ != 0 && now >= deadline_us_) break;
    if (options_.ops != 0 &&
        issued_.fetch_add(1, std::memory_order_relaxed) >= options_.ops) {
      break;
    }

    BlockKey key;
    if (!key_generator_->Next(worker.rng, &key)) break;  // seq exhausted

    uint64_t schedule_us = 0;
    if (schedule_interval_us_ > 0) {
      if (now < worker.next_schedule_us) {
        bthread_usleep(worker.next_schedule_us - now);
      }
      schedule_us = worker.next_schedule_us;
      worker.next_schedule_us += static_cast<uint64_t>(schedule_interval_us_);
    }

    {
      std::unique_lock<bthread::Mutex> lock(worker.mutex);
      while (inflight_per_worker_ != 0 &&
             worker.inflight >= inflight_per_worker_) {
        worker.cond.wait(lock);
      }
      ++worker.inflight;
    }

    if (schedule_interval_us_ == 0) {
      schedule_us = butil::gettimeofday_us();
    }

    const uint64_t bytes = operation_->BytesPerOp();
    operation_->Issue(key, [this, id, schedule_us, bytes](Status status) {
      OnComplete(id, schedule_us, bytes, status);
    });
  }

  std::unique_lock<bthread::Mutex> lock(worker.mutex);
  while (worker.inflight > 0) {
    worker.cond.wait(lock);
  }
}

void Runner::OnComplete(uint32_t id, uint64_t schedule_us, uint64_t bytes,
                        const Status& status) {
  const uint64_t latency_us = butil::gettimeofday_us() - schedule_us;
  stats_->Record(id, bytes, latency_us, status.ok());

  auto& worker = *workers_[id];
  std::lock_guard<bthread::Mutex> lock(worker.mutex);
  --worker.inflight;
  worker.cond.notify_one();
}

void Runner::Shutdown() {
  operation_.reset();
  if (block_cache_ != nullptr) {
    auto status = block_cache_->Shutdown();
    if (!status.ok()) {
      LOG(ERROR) << "Shutdown block cache failed: " << status.ToString();
    }
    block_cache_.reset();
  }
  block_accesser_.reset();
  if (mds_client_ != nullptr) {
    auto status = mds_client_->Shutdown();
    if (!status.ok()) {
      LOG(ERROR) << "Shutdown MDS client failed: " << status.ToString();
    }
    mds_client_.reset();
  }
}

}  // namespace client
}  // namespace bench
}  // namespace cache
}  // namespace dingofs
