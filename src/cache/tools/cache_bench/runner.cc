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

#include "cache/tools/cache_bench/runner.h"

#include <bthread/countdown_event.h>
#include <butil/time.h>
#include <glog/logging.h>

#include <utility>

#include "cache/tier/tier_block_cache.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/blockaccess/prefix_block_accesser.h"
#include "common/config_mapper.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace cache {
namespace {

class BlockKeyCursor {
 public:
  BlockKeyCursor(uint64_t worker_id, uint64_t block_size, uint64_t blocks)
      : worker_id_(worker_id),
        block_size_(block_size),
        blocks_(blocks),
        slice_id_(worker_id_ * blocks_ + 1) {}

  bool Valid() const { return emitted_ < blocks_; }

  BlockKey Key() const {
    return BlockKey(slice_id_, static_cast<uint32_t>(block_index_),
                    static_cast<uint32_t>(block_size_));
  }

  void Next() {
    ++emitted_;
    if (!Valid()) {
      return;
    }

    ++block_index_;
    if (block_index_ == kBlocksPerChunk) {
      ++slice_id_;
      block_index_ = 0;
    }
  }

 private:
  static constexpr uint64_t kBlocksPerChunk = 16;

  uint64_t worker_id_;
  uint64_t block_size_;
  uint64_t blocks_;
  uint64_t slice_id_;
  uint64_t block_index_{0};
  uint64_t emitted_{0};
};

}  // namespace

Runner::Runner(Options options)
    : options_(std::move(options)),
      mds_client_(std::make_shared<MDSClientImpl>()),
      reporter_(std::make_unique<ConsoleReporter>(options_, &stats_)),
      workers_(std::make_unique<utils::TaskThreadPool<>>("cache_bench")) {}

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

  RunWorkers();
  reporter_->Stop();
  Shutdown();
  return Status::OK();
}

Status Runner::Init() {
  DINGOFS_RETURN_NOT_OK(InitMdsClient());
  DINGOFS_RETURN_NOT_OK(InitBlockAccesser());
  DINGOFS_RETURN_NOT_OK(InitBlockCache());
  operation_ = NewOperation(block_cache_, options_);
  DINGOFS_RETURN_NOT_OK(InitWorkers());
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

Status Runner::InitWorkers() {
  if (workers_->Start(options_.threads) != 0) {
    return Status::Internal("start cache bench workers failed");
  }
  return Status::OK();
}

void Runner::RunWorkers() {
  bthread::CountdownEvent done(options_.threads);
  for (uint32_t i = 0; i < options_.threads; ++i) {
    workers_->Enqueue([this, i, &done]() {
      RunWorker(i);
      done.signal();
    });
  }
  done.wait();
  workers_->Stop();
}

void Runner::RunWorker(uint32_t worker_id) {
  BlockKeyCursor cursor(worker_id, options_.block_size,
                        options_.blocks_per_worker);
  for (; cursor.Valid(); cursor.Next()) {
    butil::Timer timer;

    timer.start();
    auto status = operation_->Run(cursor.Key());
    timer.stop();

    stats_.Record(operation_->BytesPerOp(), timer.u_elapsed(), status.ok());
  }
}

void Runner::Shutdown() {
  if (workers_ != nullptr) {
    workers_->Stop();
  }

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

}  // namespace cache
}  // namespace dingofs
