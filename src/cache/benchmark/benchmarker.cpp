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
 * Created Date: 2025-05-30
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/benchmarker.h"

#include <memory>

#include "cache/benchmark/option.h"
#include "cache/common/mds_client.h"
#include "cache/common/type.h"
#include "cache/storage/storage_pool.h"
#include "cache/tiercache/tier_block_cache.h"
#include "options/cache/option.h"

namespace dingofs {
namespace cache {

Benchmarker::Benchmarker()
    : mds_client_(std::make_shared<MDSClientImpl>(FLAGS_mds_addrs)),
      storage_pool_(std::make_shared<StoragePoolImpl>(mds_client_)),
      collector_(std::make_unique<Collector>()),
      reporter_(std::make_shared<Reporter>(collector_)),
      thread_pool_(std::make_unique<TaskThreadPool>("benchmarker_worker")) {
  if (FLAGS_offset + FLAGS_length > FLAGS_blksize) {
    FLAGS_length = FLAGS_blksize - FLAGS_offset;
  }
}

Status Benchmarker::Start() { return InitAll(); }

Status Benchmarker::InitAll() {
  auto initers = std::vector<std::function<Status()>>{
      [this]() { return InitMdsClient(); },
      [this]() { return InitStorage(); },
      [this]() { return InitBlockCache(); },
      [this]() { return InitCollector(); },
      [this]() { return InitReporter(); },
      [this]() {
        InitFactory();
        return Status::OK();
      },
      [this]() {
        InitWorkers();
        return Status::OK();
      }};

  for (const auto& initer : initers) {
    auto status = initer();
    if (!status.ok()) {
      return status;
    }
  }

  return Status::OK();
}

void Benchmarker::RunUntilFinish() {
  StartAll();
  StopAll();
}

void Benchmarker::StartAll() {
  StartReporter();
  StartWorkers();
}

void Benchmarker::StopAll() {
  StopWorkers();
  StopReporter();
  StopCollector();
}

// init
Status Benchmarker::InitMdsClient() { return mds_client_->Start(); }

Status Benchmarker::InitStorage() {
  auto status = storage_pool_->GetStorage(FLAGS_fsid, storage_);
  if (!status.ok()) {
    LOG(ERROR) << "Init storage failed: " << status.ToString();
    return status;
  }
  return Status::OK();
}

Status Benchmarker::InitBlockCache() {
  block_cache_ = std::make_shared<TierBlockCache>(storage_);
  auto status = block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
  }
  return status;
}

Status Benchmarker::InitCollector() {
  auto status = collector_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init collector failed: " << status.ToString();
  }
  return status;
}

Status Benchmarker::InitReporter() {
  auto status = reporter_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init reporter failed: " << status.ToString();
  }
  return status;
}

void Benchmarker::InitFactory() {
  factory_ = NewFactory(block_cache_, FLAGS_op);
}

void Benchmarker::InitWorkers() {
  CHECK_EQ(thread_pool_->Start(FLAGS_threads), 0);
  for (auto i = 0; i < FLAGS_threads; i++) {
    workers_.emplace_back(std::make_unique<Worker>(i, factory_, collector_));
  }
}

// start
void Benchmarker::StartReporter() { reporter_->Start(); }

void Benchmarker::StartWorkers() {
  for (auto& worker : workers_) {
    thread_pool_->Enqueue([&worker]() { worker->Start(); });
  }
}

// stop
void Benchmarker::StopWorkers() {
  for (auto& worker : workers_) {
    worker->Shutdown();
  }
}

void Benchmarker::StopReporter() { reporter_->Shutdown(); }

void Benchmarker::StopCollector() { collector_->Detory(); }

}  // namespace cache
}  // namespace dingofs
