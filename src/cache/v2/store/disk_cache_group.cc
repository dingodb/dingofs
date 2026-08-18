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

#include "cache/v2/store/disk_cache_group.h"

#include <glog/logging.h>

#include <utility>

namespace dingofs {
namespace cache {
namespace v2 {

DiskCacheGroup::DiskCacheGroup(const std::vector<DiskOption>& options) {
  CHECK(!options.empty()) << "a disk cache group needs disks";
  stores_.reserve(options.size());
  for (const DiskOption& option : options) {
    stores_.push_back(std::make_unique<DiskCache>(option));
  }
}

Future<> DiskCacheGroup::Start(UploadFunc uploader) {
  LOG(INFO) << "DiskCacheGroup is starting...";

  std::vector<uint64_t> caps;
  for (const DiskCacheUPtr& store : stores_) {
    co_await store->Start(uploader);
    caps.push_back(store->capacity_bytes());
  }

  const std::vector<uint32_t> weights = NormalizeGcd(caps);
  for (uint32_t i = 0; i < stores_.size(); ++i) {
    chash_.Add(i, stores_[i]->uuid(), weights[i]);
  }
  chash_.Finalize();

  LOG(INFO) << "Successfully start DiskCacheGroup{count=" << stores_.size()
            << "}";
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

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
