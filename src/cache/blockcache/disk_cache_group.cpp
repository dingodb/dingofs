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

#include <atomic>
#include <memory>

#include "cache/common/macro.h"
#include "cache/metric/cache_status.h"
#include "cache/utils/helper.h"
#include "cache/utils/ketama_con_hash.h"

namespace dingofs {
namespace cache {

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : running_(false),
      options_(options),
      chash_(std::make_unique<KetamaConHash>()),
      watcher_(std::make_unique<DiskCacheWatcher>()),
      metric_(std::make_shared<DiskCacheGroupMetric>()) {}

Status DiskCacheGroup::Start(UploadFunc uploader) {
  CHECK(!options_.empty());
  CHECK_NOTNULL(chash_);
  CHECK_NOTNULL(watcher_);
  CHECK_NOTNULL(metric_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Disk cache group is starting...";

  auto weights = CalcWeights(options_);
  for (size_t i = 0; i < options_.size(); i++) {
    auto store = std::make_shared<DiskCache>(options_[i]);
    auto status = store->Start(uploader);
    if (!status.ok()) {
      return status;
    }

    stores_[store->Id()] = store;
    chash_->AddNode(store->Id(), weights[i]);
    watcher_->Add(store, uploader);
    LOG(INFO) << "Add disk cache (dir=" << options_[i].cache_dir
              << ", weight=" << weights[i] << ") to disk cache group success.";
  }

  chash_->Final();
  watcher_->Start();

  DisplayStatus();

  running_ = true;

  LOG(INFO) << "Disk cache group is up.";

  CHECK_RUNNING("Disk cache group");
  return Status::OK();
}

Status DiskCacheGroup::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Disk cache group is shutting down...";

  watcher_->Shutdown();
  for (const auto& it : stores_) {
    auto status = it.second->Shutdown();
    if (!status.ok()) {
      return status;
    }
  }

  LOG(INFO) << "Disk cache group is down.";

  CHECK_DOWN("Disk cache group");
  return Status::OK();
}

Status DiskCacheGroup::Stage(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, StageOption option) {
  Status status;
  DiskCacheGroupMetricGuard metric_guard(__func__, block.size, status, metric_);
  status = GetStore(key)->Stage(ctx, key, block, option);

  return status;
}

Status DiskCacheGroup::RemoveStage(ContextSPtr ctx, const BlockKey& key,
                                   RemoveStageOption option) {
  CHECK_RUNNING("Disk cache group");

  DiskCacheSPtr store;
  const auto& store_id = option.block_ctx.store_id;
  if (!store_id.empty()) {
    store = GetStore(store_id);
  } else {
    store = GetStore(key);
  }
  return store->RemoveStage(ctx, key, option);
}

Status DiskCacheGroup::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, CacheOption option) {
  Status status;
  DiskCacheGroupMetricGuard metric_guard(__func__, block.size, status, metric_);
  status = GetStore(key)->Cache(ctx, key, block, option);

  return status;
}

Status DiskCacheGroup::Load(ContextSPtr ctx, const BlockKey& key, off_t offset,
                            size_t length, IOBuffer* buffer,
                            LoadOption option) {
  CHECK_RUNNING("Disk cache group");

  DiskCacheSPtr store;
  const auto& store_id = option.block_ctx.store_id;
  if (!store_id.empty()) {
    store = GetStore(store_id);
  } else {
    store = GetStore(key);
  }

  Status status;
  DiskCacheGroupMetricGuard metirc_guard(__func__, length, status, metric_);
  status = store->Load(ctx, key, offset, length, buffer, option);

  return status;
}

std::string DiskCacheGroup::Id() const { return "disk_cache_group"; }

bool DiskCacheGroup::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

bool DiskCacheGroup::IsCached(const BlockKey& key) const {
  DCHECK_RUNNING("Disk cache group");

  return GetStore(key)->IsCached(key);
}

bool DiskCacheGroup::IsFull(const BlockKey& key) const {
  DCHECK_RUNNING("Disk cache group");

  return GetStore(key)->IsFull(key);
}

std::vector<uint64_t> DiskCacheGroup::CalcWeights(
    std::vector<DiskCacheOption> options) {
  std::vector<uint64_t> weights;
  weights.reserve(options.size());
  for (const auto& option : options) {
    weights.push_back(option.cache_size_mb);
  }
  return Helper::NormalizeByGcd(weights);
}

DiskCacheSPtr DiskCacheGroup::GetStore(const BlockKey& key) const {
  ConNode node;
  bool find = chash_->Lookup(std::to_string(key.id), node);
  CHECK(find) << "No corresponding store found: key = " << key.Filename();

  auto iter = stores_.find(node.key);
  CHECK(iter != stores_.end());
  return iter->second;
}

// We should pass the request to specified store if |store_id|
// is not empty, because add/delete cache will leads the consistent hash
// changed. So when we restart the store after add/delete some stores, the
// stage block key will be mapped to one stroe by the consistent hash
// algorithm, but this is actually not the real location the block stores.
DiskCacheSPtr DiskCacheGroup::GetStore(const std::string& store_id) const {
  CHECK(!store_id.empty());
  auto iter = stores_.find(store_id);

  CHECK(iter != stores_.end())
      << "Specified store not found: store_id = " << store_id;
  return iter->second;
}

void DiskCacheGroup::DisplayStatus() const {
  CacheStatus::Update([this](CacheStatus::Root& root) {
    auto& disks = root.local_cache.disks;
    for (int i = 0; i < options_.size(); i++) {
      disks[i] = CacheStatus::Disk{
          .dir = options_[i].cache_dir,
          .capacity = options_[i].cache_size_mb,
          .health = "unknown",
      };
    }
  });
}

}  // namespace cache
}  // namespace dingofs
