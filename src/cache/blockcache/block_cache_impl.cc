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
#include "cache/blockcache/mem_cache.h"
#include "cache/common/macro.h"
#include "cache/common/storage_client.h"
#include "cache/common/storage_client_pool.h"
#include "cache/iutil/bthread.h"
#include "cache/iutil/inflight_tracker.h"
#include "cache/iutil/string_util.h"
#include "common/helper.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_store, "disk",
              "cache store type, can be none, disk or memory");
DEFINE_bool(enable_stage, true, "whether to enable stage block for writeback");
DEFINE_bool(enable_cache, true, "whether to enable cache block");

static std::vector<DiskCacheOption> ParseDiskCacheOption() {
  std::vector<std::pair<std::string, uint64_t>> cache_dirs;

  Helper::SplitUniteCacheDir(FLAGS_cache_dir, FLAGS_cache_size_mb, &cache_dirs);
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

static CacheStoreSPtr NewCacheStore() {
  if (FLAGS_cache_store == "memory") {
    return std::make_shared<MemCache>(
        MemCacheOption{.cache_size_mb = FLAGS_cache_size_mb});
  }
  return std::make_shared<DiskCacheGroup>(ParseDiskCacheOption());
}

BlockCacheImpl::BlockCacheImpl(StorageClient* storage_client)
    : BlockCacheImpl(std::make_shared<SingletonStorageClient>(storage_client)) {
}

BlockCacheImpl::BlockCacheImpl(StorageClientPoolSPtr storage_client_pool)
    : running_(false),
      storage_client_pool_(storage_client_pool),
      store_(NewCacheStore()),
      uploader_(
          std::make_shared<BlockCacheUploader>(store_, storage_client_pool_)),
      joiner_(std::make_unique<iutil::BthreadJoiner>()),
      cache_tracker_(std::make_shared<iutil::InflightTracker>(1024)),
      prefetch_tracker_(std::make_shared<iutil::InflightTracker>(1024)) {}

BlockCacheImpl::~BlockCacheImpl() { Shutdown(); }

Status BlockCacheImpl::Start() {
  CHECK(FLAGS_cache_store == "disk" || FLAGS_cache_store == "memory")
      << "unsupported cache_store: " << FLAGS_cache_store;

  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "BlockCacheImpl is already started";
    return Status::OK();
  }

  LOG(INFO) << "BlockCacheImpl is starting...";

  uploader_->Start();

  auto status = store_->Start(
      [this](BlockHandle handle, size_t length, BlockAttr block_attr) {
        uploader_->EnterUploadQueue(StageBlock(handle, length, block_attr));
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
  LOG(INFO) << "Successfully shutdown BlockCacheImpl";
  return Status::OK();
}

Status BlockCacheImpl::Put(BlockHandle handle, IOBuffer block,
                           PutOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  size_t length = block.Size();
  auto status = store_->Stage(handle, std::move(block),
                              {.block_attr = option.block_attr});
  if (status.ok()) {
    return status;
  } else if (status.IsCacheFull()) {
    LOG_EVERY_SECOND(WARNING)
        << "Stage block failed: key = " << handle.Filename()
        << ", length = " << length << ", status = " << status.ToString();
  } else {
    LOG(ERROR) << "Fail to stage block key=" << handle.Filename();
  }

  return status;
}

Status BlockCacheImpl::Range(BlockHandle handle, off_t offset, size_t length,
                             IOBuffer* buffer, RangeOption /*option*/) {
  DCHECK_RUNNING("BlockCacheImpl");

  return store_->Load(std::move(handle), offset, length, buffer);
}

Status BlockCacheImpl::Cache(BlockHandle handle, IOBuffer block,
                             CacheOption /*option*/) {
  DCHECK_RUNNING("BlockCacheImpl");

  size_t length = block.Size();
  auto status = store_->Cache(handle, std::move(block));
  if (status.IsCacheFull()) {
    LOG_EVERY_SECOND(WARNING)
        << "Cache block failed: key = " << handle.Filename()
        << ", length = " << length << ", status = " << status.ToString();
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block key=" << handle.Filename();
  }

  return status;
}

Status BlockCacheImpl::Prefetch(BlockHandle handle, size_t length,
                                PrefetchOption /*option*/) {
  DCHECK_RUNNING("BlockCacheImpl");

  if (IsCached(handle)) {
    return Status::OK();
  } else if (store_->IsFull(handle)) {
    return Status::CacheFull("disk cache is full");
  }

  // StorageRange -> StorageClient::Range -> IOBuffer::Fetch1() requires the
  // output buffer to be a single, pre-allocated backing block.
  IOBuffer buffer;
  char* data = new char[length];
  buffer.AppendUserData(data, length, iutil::DeleteBuffer);
  auto status = StorageRange(handle, 0, length, &buffer);
  if (!status.ok()) {
    return status;
  }

  status = store_->Cache(handle, std::move(buffer));
  if (!status.ok()) {
    LOG(ERROR) << "Fail to prefetch block key=" << handle.Filename();
  }
  return status;
}

void BlockCacheImpl::AsyncPut(BlockHandle handle, IOBuffer block,
                              AsyncCallback cb, PutOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto* self = GetSelfPtr();
  auto tid =
      iutil::RunInBthread([self, handle = std::move(handle),
                           block = std::move(block), cb, option]() mutable {
        Status status = self->Put(std::move(handle), std::move(block), option);
        if (cb) {
          cb(status);
        }
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheImpl::AsyncRange(BlockHandle handle, off_t offset, size_t length,
                                IOBuffer* buffer, AsyncCallback cb,
                                RangeOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, handle = std::move(handle), offset,
                                  length, buffer, cb, option]() mutable {
    Status status =
        self->Range(std::move(handle), offset, length, buffer, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheImpl::AsyncCache(BlockHandle handle, IOBuffer block,
                                AsyncCallback cb, CacheOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto tracker = cache_tracker_;
  auto status = tracker->Add(handle.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [tracker, self, handle, block = std::move(block), cb, option]() mutable {
        Status status = self->Cache(handle, std::move(block), option);
        if (cb) {
          cb(status);
        }
        tracker->Remove(handle.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void BlockCacheImpl::AsyncPrefetch(BlockHandle handle, size_t length,
                                   AsyncCallback cb, PrefetchOption option) {
  DCHECK_RUNNING("BlockCacheImpl");

  auto tracker = prefetch_tracker_;
  auto status = tracker->Add(handle.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [tracker, self, handle, length, cb, option]() mutable {
        Status status = self->Prefetch(handle, length, option);
        if (cb) {
          cb(status);
        }
        tracker->Remove(handle.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

Status BlockCacheImpl::StoragePut(const BlockHandle& handle,
                                  const IOBuffer& block) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(handle.FsId(), &storage_client);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get storage client for key=" << handle.Filename();
    return status;
  }

  status = storage_client->Put(handle, block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to storage, key=" << handle.Filename();
  }
  return status;
}

Status BlockCacheImpl::StorageRange(const BlockHandle& handle, off_t offset,
                                    size_t length, IOBuffer* buffer) {
  StorageClient* storage_client;
  auto status =
      storage_client_pool_->GetStorageClient(handle.FsId(), &storage_client);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to get storage client for key=" << handle.Filename();
    return status;
  }

  status = storage_client->Range(handle, offset, length, buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to range block from storage, key=" << handle.Filename();
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
