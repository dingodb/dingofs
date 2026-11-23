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

#include "blockcache/store/disk_cache_group.h"

#include <glog/logging.h>

#include <utility>

namespace dingofs {
namespace blockcache {

DiskCacheGroup::DiskCacheGroup(const std::vector<DiskOption>& options) {
  CHECK(!options.empty()) << "a disk cache group needs disks";
  stores_.reserve(options.size());
  for (const DiskOption& option : options) {
    stores_.push_back(std::make_unique<DiskCache>(option));
  }
}

Future<Status> DiskCacheGroup::Start(UploadFunc uploader) {
  LOG(INFO) << "DiskCacheGroup is starting...";

  for (const DiskCacheUPtr& store : stores_) {
    const Status status = co_await store->Start(uploader);
    if (!status.ok()) {
      co_return status;
    }
  }

  for (const DiskCacheUPtr& store : stores_) {
    chash_.Add(store->uuid(), store->capacity_bytes());
    CHECK(stores_by_uuid_.emplace(store->uuid(), store.get()).second)
        << "two disks in cache_dir share uuid " << store->uuid()
        << ": a dir listed twice, or a copied .lock file";
  }
  chash_.Finalize();

  LOG(INFO) << "Successfully start DiskCacheGroup{count=" << stores_.size()
            << "}";
  co_return Status::OK();
}

Future<> DiskCacheGroup::Shutdown() {
  LOG(INFO) << "DiskCacheGroup is shutting down...";

  for (const DiskCacheUPtr& store : stores_) {
    co_await store->Shutdown();
  }

  LOG(INFO) << "Successfully shutdown DiskCacheGroup";
}

Future<Status> DiskCacheGroup::Stage(BlockHandle handle, BufferViews block) {
  return GetStore(handle).Stage(handle, block);
}

Future<Status> DiskCacheGroup::RemoveStage(BlockHandle handle) {
  return GetStore(handle).RemoveStage(handle);
}

Future<Status> DiskCacheGroup::Cache(BlockHandle handle, BufferViews block) {
  return GetStore(handle).Cache(handle, block);
}

Future<Status> DiskCacheGroup::Load(BlockHandle handle, uint64_t offset,
                                    uint32_t length, char* buffer) {
  return GetStore(handle).Load(handle, offset, length, buffer);
}

Future<Status> DiskCacheGroup::Delete(BlockHandle handle) {
  return GetStore(handle).Delete(handle);
}

Future<bool> DiskCacheGroup::Exists(BlockHandle handle) {
  return GetStore(handle).Exists(handle);
}

Future<CacheStats> DiskCacheGroup::GetStats() {
  CacheStats stats;
  for (const DiskCacheUPtr& store : stores_) {
    stats.Merge(co_await store->GetStats());
  }
  co_return stats;
}

}  // namespace blockcache
}  // namespace dingofs
