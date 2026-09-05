/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "blockcache/tools/benchmark/benchmarker.h"

#include <glog/logging.h>

#include <cstdlib>
#include <functional>

#include "blockcache/api/memory.h"
#include "blockcache/object/object.h"
#include "blockcache/tools/benchmark/option.h"

namespace dingofs {
namespace blockcache {

Benchmarker::Benchmarker()
    : collector_(std::make_shared<Collector>(FLAGS_threads)),
      reporter_(std::make_shared<Reporter>(collector_)),
      thread_pool_(
          std::make_unique<utils::TaskThreadPool<>>("benchmark_worker")) {
  CHECK_LT(FLAGS_offset, FLAGS_blksize);
  if (FLAGS_offset + FLAGS_length > FLAGS_blksize) {
    FLAGS_length = FLAGS_blksize - FLAGS_offset;
  }
  CHECK_GT(FLAGS_length, 0);
}

Status Benchmarker::Start() { return InitAll(); }

void Benchmarker::RunUntilFinish() {
  StartAll();
  StopAll();
}

Status Benchmarker::InitAll() {
  auto initers = std::vector<std::function<Status()>>{
      [this]() { return InitMdsClient(); },
      [this]() { return InitStorage(); },
      [this]() { return InitBlockCache(); },
      [this]() { return InitBuffers(); },
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

Status Benchmarker::InitMdsClient() {
  mds_client_ = std::make_unique<MDSClientImpl>();
  auto status = mds_client_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init mds client failed: " << status.ToString();
  }
  return status;
}

Status Benchmarker::InitStorage() {
  storage_client_ = std::make_unique<StorageClient>(mds_client_.get());
  storage_client_->Start();
  return Status::OK();
}

Status Benchmarker::InitBlockCache() {
  block_cache_ = std::make_unique<BlockCacheImpl>(
      mds_client_.get(),
      std::make_unique<ObjectStorage>(storage_client_.get()));
  auto status = block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
  }
  return status;
}

Status Benchmarker::InitBuffers() {
  size_t bytes =
      static_cast<size_t>(FLAGS_threads) * FLAGS_iodepth * FLAGS_blksize;
  bytes = (bytes + 4095) & ~size_t{4095};
  buffers_ = static_cast<char*>(std::aligned_alloc(4096, bytes));
  if (buffers_ == nullptr) {
    return Status::Internal("allocate benchmark buffers failed");
  }
  return RegisterMemoryForRDMA(buffers_, bytes);
}

void Benchmarker::InitFactory() {
  factory_ = NewFactory(block_cache_.get(), FLAGS_op);
}

void Benchmarker::InitWorkers() {
  CHECK_EQ(thread_pool_->Start(static_cast<int>(FLAGS_threads)), 0);
  for (uint32_t i = 0; i < FLAGS_threads; i++) {
    char* buffer =
        buffers_ + (static_cast<size_t>(i) * FLAGS_iodepth * FLAGS_blksize);
    workers_.emplace_back(
        std::make_unique<Worker>(i, buffer, factory_, collector_->SlotAt(i)));
  }
}

void Benchmarker::StartAll() {
  StartReporter();
  StartWorkers();
}

void Benchmarker::StartReporter() {
  auto status = reporter_->Start();
  CHECK(status.ok()) << "Start reporter failed: " << status.ToString();
}

void Benchmarker::StartWorkers() {
  for (auto& worker : workers_) {
    thread_pool_->Enqueue([&worker]() { worker->Start(); });
  }
}

void Benchmarker::StopAll() {
  StopWorkers();
  StopReporter();
  StopBlockCache();
}

void Benchmarker::StopWorkers() {
  for (auto& worker : workers_) {
    worker->Shutdown();
  }
  thread_pool_->Stop();
}

void Benchmarker::StopReporter() { reporter_->Shutdown(); }

void Benchmarker::StopBlockCache() {
  block_cache_->Shutdown();
  storage_client_->Shutdown();
  mds_client_->Shutdown();
  std::free(buffers_);
  buffers_ = nullptr;
}

}  // namespace blockcache
}  // namespace dingofs
