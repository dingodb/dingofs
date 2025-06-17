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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/disk_cache_group.h"

#include "base/hash/ketama_con_hash.h"
#include "cache/utils/helper.h"

namespace dingofs {
namespace cache {

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : running_(false),
      options_(options),
      chash_(std::make_unique<base::hash::KetamaConHash>()),
      watcher_(std::make_unique<DiskCacheWatcher>()) {}

Status DiskCacheGroup::Init(UploadFunc uploader) {
  if (!running_.exchange(true)) {
    auto weights = CalcWeights(options_);
    for (size_t i = 0; i < options_.size(); i++) {
      auto store = std::make_shared<DiskCache>(options_[i]);
      auto status = store->Init(uploader);
      if (!status.ok()) {
        return status;
      }

      stores_[store->Id()] = store;
      chash_->AddNode(store->Id(), weights[i]);
      watcher_->Add(store, uploader);
      LOG(INFO) << "Add disk cache (dir=" << options_[i].cache_dir
                << ", weight=" << weights[i]
                << ") to disk cache group success.";
    }

    chash_->Final();
    watcher_->Start();
  }
  return Status::OK();
}

Status DiskCacheGroup::Shutdown() {
  if (running_.exchange(false)) {
    watcher_->Stop();
    for (const auto& it : stores_) {
      auto status = it.second->Shutdown();
      if (!status.ok()) {
        return status;
      }
    }
  }
  return Status::OK();
}

Status DiskCacheGroup::Stage(const BlockKey& key, const Block& block,
                             StageOption option) {
  return GetStore(key)->Stage(key, block, option);
}

Status DiskCacheGroup::RemoveStage(const BlockKey& key,
                                   RemoveStageOption option) {
  DiskCacheSPtr store;
  const auto& store_id = option.ctx.store_id;
  if (!store_id.empty()) {
    store = GetStore(store_id);
  } else {
    store = GetStore(key);
  }
  return store->RemoveStage(key, option);
}

Status DiskCacheGroup::Cache(const BlockKey& key, const Block& block,
                             CacheOption option) {
  return GetStore(key)->Cache(key, block, option);
}

Status DiskCacheGroup::Load(const BlockKey& key, off_t offset, size_t length,
                            IOBuffer* buffer, LoadOption option) {
  DiskCacheSPtr store;
  const auto& store_id = option.ctx.store_id;
  if (!store_id.empty()) {
    store = GetStore(store_id);
  } else {
    store = GetStore(key);
  }
  return store->Load(key, offset, length, buffer, option);
}

bool DiskCacheGroup::IsRunning() const {
  return running_.load(std::memory_order_acquire);
}

bool DiskCacheGroup::IsCached(const BlockKey& key) const {
  return GetStore(key)->IsCached(key);
}

std::string DiskCacheGroup::Id() const { return "disk_cache_group"; }

std::vector<uint64_t> DiskCacheGroup::CalcWeights(
    std::vector<DiskCacheOption> options) {
  std::vector<uint64_t> weights;
  for (const auto& option : options) {
    weights.push_back(option.cache_size_mb);
  }
  return Helper::NormalizeByGcd(weights);
}

DiskCacheSPtr DiskCacheGroup::GetStore(const BlockKey& key) const {
  base::hash::ConNode node;
  bool find = chash_->Lookup(std::to_string(key.id), node);
  CHECK(find);

  auto iter = stores_.find(node.key);
  CHECK(iter != stores_.end());
  return iter->second;
}

// We should pass the request to specified store if |store_id|
// is not empty, because add/delete cache will leads the consistent hash
// changed.
// so when we restart the store after add/delete some stores, the
// stage block key will be mapped to one stroe by the consistent hash
// algorithm, but this is actually not the real location the block stores.
DiskCacheSPtr DiskCacheGroup::GetStore(const std::string& store_id) const {
  CHECK(!store_id.empty());

  auto iter = stores_.find(store_id);
  CHECK(iter != stores_.end());
  return iter->second;
}

}  // namespace cache
}  // namespace dingofs
