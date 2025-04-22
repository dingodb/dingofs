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

#include <cassert>
#include <memory>
#include <numeric>

#include "base/hash/ketama_con_hash.h"
#include "base/math/math.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/blockcache/disk_cache_metric.h"
#include "cache/blockcache/disk_cache_watcher.h"
#include "cache/utils/local_filesystem.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using base::hash::ConNode;
using base::hash::KetamaConHash;
using DiskCacheTotalMetric = ::dingofs::stub::metric::DiskCacheMetric;

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : options_(options),
      chash_(std::make_unique<KetamaConHash>()),
      watcher_(std::make_unique<DiskCacheWatcher>()) {}

Status DiskCacheGroup::Init(UploadFunc uploader) {
  auto weights = CalcWeights(options_);
  for (size_t i = 0; i < options_.size(); i++) {
    auto store = std::make_shared<DiskCache>(options_[i]);
    auto status = store->Init(uploader);
    if (!status.ok()) {
      return status;
    }

    stores_[store->Id()] = store;
    chash_->AddNode(store->Id(), weights[i]);
    watcher_->Add(options_[i].cache_dir(), store);
    LOG(INFO) << "Add disk cache (dir=" << options_[i].cache_dir()
              << ", weight=" << weights[i] << ") to disk cache group success.";
  }

  chash_->Final();
  watcher_->Start(uploader);
  return Status::OK();
}

Status DiskCacheGroup::Shutdown() {
  for (const auto& it : stores_) {
    auto status = it.second->Shutdown();
    if (!status.ok()) {
      return status;
    }
  }
  watcher_->Stop();
  return Status::OK();
}

Status DiskCacheGroup::Stage(const BlockKey& key, const Block& block,
                             BlockContext ctx) {
  Status status;
  DiskCacheMetricGuard guard(
      &status, &DiskCacheTotalMetric::GetInstance().write_disk, block.size);
  status = GetStore(key)->Stage(key, block, ctx);
  return status;
}

Status DiskCacheGroup::RemoveStage(const BlockKey& key, BlockContext ctx) {
  auto store = GetStore(key);

  // We should pass the request to specified store if |ctx.store_id|
  // is not empty, because add/delete cache will leads the consistent hash
  // changed. so when we restart the store after add/delete some stores, the
  // stage block will be reloaded by one store to upload, but the RemoveStage
  // request maybe pass to another store to handle after upload success.
  if (!ctx.store_id.empty()) {
    store = stores_[ctx.store_id];
    CHECK(store != nullptr);
  }
  return store->RemoveStage(key, ctx);
}

Status DiskCacheGroup::Cache(const BlockKey& key, const Block& block) {
  Status status;
  DiskCacheMetricGuard guard(
      &status, &DiskCacheTotalMetric::GetInstance().write_disk, block.size);
  status = GetStore(key)->Cache(key, block);
  return status;
}

Status DiskCacheGroup::Load(const BlockKey& key,
                            std::shared_ptr<BlockReader>& reader) {
  return GetStore(key)->Load(key, reader);
}

bool DiskCacheGroup::IsCached(const BlockKey& key) {
  return GetStore(key)->IsCached(key);
}

std::string DiskCacheGroup::Id() { return "disk_cache_group"; }

std::vector<uint64_t> DiskCacheGroup::CalcWeights(
    std::vector<DiskCacheOption> options) {
  uint64_t gcd = 0;
  std::vector<uint64_t> weights;
  for (const auto& option : options) {
    weights.push_back(option.cache_size_mb());
    gcd = std::gcd(gcd, option.cache_size_mb());
  }
  assert(gcd != 0);

  for (auto& weight : weights) {
    weight = weight / gcd;
  }
  return weights;
}

std::shared_ptr<DiskCache> DiskCacheGroup::GetStore(const BlockKey& key) {
  ConNode node;
  bool find = chash_->Lookup(std::to_string(key.id), node);
  assert(find);

  auto it = stores_.find(node.key);
  assert(it != stores_.end());
  return it->second;
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
