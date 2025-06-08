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

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/remotecache/remote_block_cache.h"
#include "cache/storage/storage_impl.h"

namespace dingofs {
namespace cache {

extern BenchmarkOption* g_option;

static blockaccess::BlockAccesserSPtr NewBlockAccesser() {
  blockaccess::S3Options s3_option;
  s3_option.s3_info.ak = g_option->ak;
  s3_option.s3_info.sk = g_option->sk;
  s3_option.s3_info.endpoint = g_option->endpoint;
  s3_option.s3_info.bucket_name = g_option->bucket;

  blockaccess::BlockAccessOptions option;
  option.type = blockaccess::AccesserType::kS3;
  option.s3_options = s3_option;
  return std::make_shared<blockaccess::BlockAccesserImpl>(option);
}

static BlockCacheSPtr NewLocalBlockCache() {
  // block accesser
  auto block_accesser = NewBlockAccesser();

  // storage
  auto storage = std::make_shared<StorageImpl>(block_accesser);

  auto block_cache =
      std::make_shared<BlockCacheImpl>(g_option->block_cache_option, storage);
  return block_cache;
}

static BlockCacheSPtr NewRemoteBlockCache() {
  auto remote_block_cache = std::make_shared<RemoteBlockCacheImpl>(
      g_option->remote_block_cache_option);
  return remote_block_cache;
}

Benchmarker::Benchmarker()
    : task_pool_(std::make_unique<TaskThreadPool>("benchmarker")),
      reporter_(std::make_shared<Reporter>()) {
  if (g_option->local) {
    block_cache_ = NewLocalBlockCache();
  } else {
    block_cache_ = NewRemoteBlockCache();
  }
}

Status Benchmarker::Run() {
  // block cache
  auto status = block_cache_->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
    return status;
  }

  // task pool
  CHECK_EQ(task_pool_->Start(g_option->threads), 0);

  // create worker
  for (auto i = 0; i < g_option->threads; i++) {
    auto worker = std::make_shared<Worker>(i, block_cache_, reporter_);
    auto status = worker->Init();
    if (!status.ok()) {
      return status;
    }
    workers_.emplace_back(worker);
  }

  // reporter
  status = reporter_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Init reporter failed: " << status.ToString();
    return Status::Internal("init reporter failed");
  }

  // start workers
  for (auto& worker : workers_) {
    task_pool_->Enqueue([worker]() { worker->Run(); });
  }

  return Status::OK();
}

void Benchmarker::Stop() {
  std::this_thread::sleep_for(std::chrono::seconds(3600));
}

}  // namespace cache
}  // namespace dingofs