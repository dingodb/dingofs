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

#include "base/time/time.h"
#include "blockaccess/block_access_log.h"
#include "cache/config/config.h"
#include "cache/tiercache/tier_block_cache.h"
#include "cache/utils/access_log.h"

namespace dingofs {
namespace cache {

Benchmarker::Benchmarker()
    : block_accesser_(std::make_unique<blockaccess::BlockAccesserImpl>(
          NewBlockAccessOptions())),
      block_cache_(std::make_shared<TierBlockCache>(
          BlockCacheOption(), RemoteBlockCacheOption(), block_accesser_.get())),
      task_pool_(std::make_unique<TaskThreadPool>("benchmarker")),
      countdown_event_(std::make_shared<BthreadCountdownEvent>(FLAGS_threads)),
      reporter_(std::make_shared<Reporter>()) {
  if (FLAGS_ino == 0) {
    FLAGS_ino = base::time::TimeNow().seconds;
  }
}

Status Benchmarker::Run() {
  // Init logger, block cache, workers
  auto status = Init();
  if (!status.ok()) {
    return status;
  }

  // Start reporter, workers
  status = Start();
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

void Benchmarker::Shutdown() {
  // Wait for all workers to complete
  countdown_event_->wait();

  LOG(INFO) << "All workers completed, shutting down...";

  // stop worker, reporter
  Stop();
}

Status Benchmarker::Init() {
  auto status = InitBlockCache();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
    return status;
  }

  status = InitWrokers();
  if (!status.ok()) {
    LOG(ERROR) << "Init workers failed: " << status.ToString();
    return status;
  }

  return Status::OK();
}

Status Benchmarker::InitBlockCache() {
  auto status = block_accesser_->Init();
  if (!status.ok()) {
    return status;
  }
  return block_cache_->Init();
}

Status Benchmarker::InitWrokers() {
  for (auto i = 0; i < FLAGS_threads; i++) {
    auto worker =
        std::make_shared<Worker>(i, block_cache_, reporter_, countdown_event_);
    auto status = worker->Init();
    if (!status.ok()) {
      return status;
    }

    workers_.emplace_back(worker);
  }
  return Status::OK();
}

Status Benchmarker::Start() {
  auto status = StartReporter();
  if (!status.ok()) {
    LOG(ERROR) << "Start reporter failed: " << status.ToString();
    return status;
  }

  StartWorkers();

  return Status::OK();
}

Status Benchmarker::StartReporter() { return reporter_->Start(); }

void Benchmarker::StartWorkers() {
  CHECK_EQ(task_pool_->Start(FLAGS_threads), 0);

  for (auto& worker : workers_) {
    task_pool_->Enqueue([worker]() { worker->Run(); });
  }
}

void Benchmarker::Stop() {
  StopWorkers();
  StopReporter();
  StopBlockCache();
}

void Benchmarker::StopWorkers() {
  for (auto& worker : workers_) {
    worker->Shutdown();
  }
}

void Benchmarker::StopReporter() {
  auto status = reporter_->Stop();
  if (!status.ok()) {
    LOG(ERROR) << "Stop reporter failed: " << status.ToString();
  }
}

void Benchmarker::StopBlockCache() {
  auto status = block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown block cache failed: " << status.ToString();
  }
}

}  // namespace cache
}  // namespace dingofs
