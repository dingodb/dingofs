/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_impl.h"

#include <absl/strings/str_format.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <utility>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache.h"
#include "cache/blockcache/disk_cache_group.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/common/context.h"
#include "cache/common/macro.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "cache/iutil/bthread.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_store, "disk",
              "cache store type, can be none, disk or 3fs");
DEFINE_bool(enable_stage, true, "whether to enable stage block for writeback");
DEFINE_bool(enable_cache, true, "whether to enable cache block");

static void SplitUniteCacheDir(
    const std::string& cache_dir, uint64_t default_cache_size_mb,
    std::vector<std::pair<std::string, uint64_t>>* cache_dirs) {
  std::vector<std::string> dirs = absl::StrSplit(cache_dir, ",");

  for (const auto& dir : dirs) {
    uint64_t cache_size_mb = default_cache_size_mb;
    std::vector<std::string> items = absl::StrSplit(dir, ":");
    if (items.size() > 2 ||
        (items.size() == 2 && !utils::Str2Int(items[1], &cache_size_mb))) {
      CHECK(false) << "Invalid cache dir: " << dir;
    } else if (cache_size_mb == 0) {
      CHECK(false) << "Cache size must greater than 0.";
    }

    cache_dirs->emplace_back(items[0], cache_size_mb);
  }
}

static std::vector<DiskCacheOption> ParseDiskCacheOption() {
  std::vector<std::pair<std::string, uint64_t>> cache_dirs;

  SplitUniteCacheDir(FLAGS_cache_dir, FLAGS_cache_size_mb, &cache_dirs);
  CHECK(!FLAGS_cache_dir_uuid.empty())
      << "cache_dir_uuid MUST be set for disk cache";

  std::vector<DiskCacheOption> disk_cache_options;
  DiskCacheOption option;
  for (auto i = 0; i < cache_dirs.size(); i++) {
    option.cache_store = FLAGS_cache_store;
    option.cache_index = disk_cache_options.size();
    option.cache_dir = cache::RealCacheDir(
        std::filesystem::absolute(cache_dirs[i].first), FLAGS_cache_dir_uuid);
    option.cache_size_mb = cache_dirs[i].second;
    disk_cache_options.emplace_back(option);
  }

  return disk_cache_options;
}

BlockCacheImpl::BlockCacheImpl(StorageClient* storage_client)
    : BlockCacheImpl(std::make_shared<SingletonStorageClient>(storage_client)) {
}

BlockCacheImpl::BlockCacheImpl(StorageClientPoolSPtr storage_client_pool)
    : running_(false),
      storage_client_pool_(storage_client_pool),
      store_(std::make_shared<DiskCacheGroup>(ParseDiskCacheOption())),
      uploader_(
          std::make_shared<BlockCacheUploader>(store_, storage_client_pool_)),
      joiner_(std::make_unique<iutil::BthreadJoiner>()),
      cache_tracker_(std::make_shared<iutil::InflightTracker>(1024)),
      prefetch_tracker_(std::make_shared<iutil::InflightTracker>(1024)) {}

BlockCacheImpl::~BlockCacheImpl() { Shutdown(); }

Status BlockCacheImpl::Start() {
  CHECK_EQ(FLAGS_cache_store, "disk");

  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "BlockCacheImpl is already started";
    return Status::OK();
  }

  LOG(INFO) << "BlockCacheImpl is starting...";

  uploader_->Start();

  auto status = store_->Start([this](ContextSPtr ctx, const BlockKey& key,
                                     size_t length, BlockAttr block_attr) {
    uploader_->EnterUploadQueue(StageBlock(ctx, key, length, block_attr));
  });
  if (!status.ok()) {
    LOG(ERROR) << "Fail to init DiskCache";
    return status;
  }

  joiner_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "BlockCacheImpl started";
  return Status::OK();
}

Status BlockCacheImpl::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    return Status::OK();
  }

  LOG(INFO) << "BlockCacheImpl is shutting down...";

  joiner_->Shutdown();
  uploader_->Shutdown();
  store_->Shutdown();

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "BlockCacheImpl is down";
  return Status::OK();
}

Status BlockCacheImpl::Put(ContextSPtr ctx, const BlockKey& key,
                           const Block& block, PutOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  // writeback: stage block
  auto status =
      store_->Stage(ctx, key, block, {.block_attr = option.block_attr});
  if (status.ok()) {
    return status;
  } else if (status.IsCacheFull()) {
    LOG_EVERY_SECOND(WARNING)
        << "Stage block failed:  key = " << key.Filename()
        << ", length = " << block.size << ", status = " << status.ToString();
  } else {
    LOG(ERROR) << "Fail to stage block key=" << key.Filename();
  }

  return status;
}

Status BlockCacheImpl::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                             size_t length, IOBuffer* buffer,
                             RangeOption /*option*/) {
  DCHECK_RUNNING("BlockCacheImpl");

  return store_->Load(ctx, key, offset, length, buffer);
}

Status BlockCacheImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, CacheOption /*option*/) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto status = store_->Cache(ctx, key, block);
  if (status.IsCacheFull()) {
    LOG_EVERY_SECOND(WARNING)
        << "Cache block failed: key = " << key.Filename()
        << ", length = " << block.size << ", status = " << status.ToString();
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block key=" << key.Filename();
  }

  return status;
}

Status BlockCacheImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length, PrefetchOption /*option*/) {
  DCHECK_RUNNING("BlockCacheImpl");

  if (IsCached(key)) {
    return Status::OK();
  } else if (store_->IsFull(key)) {
    return Status::CacheFull("disk cache is full");
  }

  IOBuffer buffer;
  auto status = StorageRange(ctx, key, 0, length, &buffer);
  if (!status.ok()) {
    return status;
  }

  status = store_->Cache(ctx, key, Block(buffer));
  if (!status.ok()) {
    LOG(ERROR) << "Fail to prefetch block key=" << key.Filename();
  }
  return status;
}

void BlockCacheImpl::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                              const Block& block, AsyncCallback cb,
                              PutOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, ctx, key, block, cb, option]() {
    Status status = self->Put(ctx, key, block, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheImpl::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                off_t offset, size_t length, IOBuffer* buffer,
                                AsyncCallback cb, RangeOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [self, ctx, key, offset, length, buffer, cb, option]() {
        Status status = self->Range(ctx, key, offset, length, buffer, option);
        if (cb) {
          cb(status);
        }
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                const Block& block, AsyncCallback cb,
                                CacheOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto tracker = cache_tracker_;
  auto status = tracker->Add(key.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid =
      iutil::RunInBthread([tracker, self, ctx, key, block, cb, option]() {
        Status status = self->Cache(ctx, key, block, option);
        if (cb) {
          cb(status);
        }

        tracker->Remove(key.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheImpl::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                   size_t length, AsyncCallback cb,
                                   PrefetchOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto tracker = prefetch_tracker_;
  auto status = tracker->Add(key.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid =
      iutil::RunInBthread([tracker, self, ctx, key, length, cb, option]() {
        Status status = self->Prefetch(ctx, key, length, option);
        if (cb) {
          cb(status);
        }

        tracker->Remove(key.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

Status BlockCacheImpl::StoragePut(ContextSPtr ctx, const BlockKey& key,
                                  const Block& block) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(key.fs_id, &storage_client);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get storage client for key=" << key.Filename();
    return status;
  }

  status = storage_client->Put(ctx, key, &block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to storage, key=" << key.Filename();
  }
  return status;
}

Status BlockCacheImpl::StorageRange(ContextSPtr ctx, const BlockKey& key,
                                    off_t offset, size_t length,
                                    IOBuffer* buffer) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(key.fs_id, &storage_client);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get storage client for key=" << key.Filename();
    return status;
  }

  status = storage_client->Range(ctx, key, offset, length, buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to range block from storage, key=" << key.Filename();
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
