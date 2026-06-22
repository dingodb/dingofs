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

#include "cache/local/disk_cache_group.h"

#include <atomic>
#include <memory>

#include "cache/common/macro.h"
#include "cache/iutil/ketama_con_hash.h"
#include "cache/iutil/math_util.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

DiskCacheGroup::DiskCacheGroup(std::vector<DiskCacheOption> options)
    : running_(false),
      options_(options),
      chash_(std::make_unique<iutil::KetamaConHash>()),
      watcher_(std::make_unique<DiskCacheWatcher>()),
      vars_(std::make_shared<DiskCacheGroupMetrics>()) {}

Status DiskCacheGroup::Start(UploadFunc uploader) {
  CHECK(!options_.empty());
  CHECK_NOTNULL(chash_);
  CHECK_NOTNULL(watcher_);
  CHECK_NOTNULL(vars_);

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
  return Status::OK();
}

Status DiskCacheGroup::Stage(BlockHandle handle, IOBuffer block,
                             StageOption option) {
  Status status;
  size_t length = block.Size();
  DiskCacheGroupMetricsGuard metric_guard(__func__, length, status, vars_);
  auto store = GetStore(handle);
  status = store->Stage(std::move(handle), std::move(block), option);

  return status;
}

Status DiskCacheGroup::RemoveStage(BlockHandle handle,
                                   RemoveStageOption option) {
  CHECK_RUNNING("Disk cache group");

  DiskCacheSPtr store;
  const auto& store_id = option.block_attr.store_id;
  if (!store_id.empty()) {
    store = GetStore(store_id);
  } else {
    store = GetStore(handle);
  }
  return store->RemoveStage(std::move(handle), option);
}

Status DiskCacheGroup::Cache(BlockHandle handle, IOBuffer block,
                             CacheOption option) {
  Status status;
  size_t length = block.Size();
  DiskCacheGroupMetricsGuard metric_guard(__func__, length, status, vars_);
  auto store = GetStore(handle);
  status = store->Cache(std::move(handle), std::move(block), option);

  return status;
}

Status DiskCacheGroup::Load(BlockHandle handle, off_t offset, size_t length,
                            IOBuffer* buffer, LoadOption option) {
  CHECK_RUNNING("Disk cache group");

  DiskCacheSPtr store;
  const auto& store_id = option.block_attr.store_id;
  if (!store_id.empty()) {
    store = GetStore(store_id);
  } else {
    store = GetStore(handle);
  }

  Status status;
  DiskCacheGroupMetricsGuard metirc_guard(__func__, length, status, vars_);
  status = store->Load(std::move(handle), offset, length, buffer, option);

  return status;
}

std::string DiskCacheGroup::Id() const { return "disk_cache_group"; }

bool DiskCacheGroup::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

bool DiskCacheGroup::IsCached(const BlockHandle& handle) const {
  DCHECK_RUNNING("Disk cache group");

  return GetStore(handle)->IsCached(handle);
}

bool DiskCacheGroup::IsFull(const BlockHandle& handle) const {
  DCHECK_RUNNING("Disk cache group");

  return GetStore(handle)->IsFull(handle);
}

std::vector<uint64_t> DiskCacheGroup::CalcWeights(
    std::vector<DiskCacheOption> options) {
  std::vector<uint64_t> weights;
  weights.reserve(options.size());
  for (const auto& option : options) {
    weights.push_back(option.cache_size_mb);
  }
  return iutil::NormalizeByGcd(weights);
}

DiskCacheSPtr DiskCacheGroup::GetStore(const BlockHandle& handle) const {
  iutil::ConNode node;
  bool find = chash_->Lookup(handle.Id(), node);
  CHECK(find) << "No corresponding store found: key = " << handle.Filename();

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

bool DiskCacheGroup::Dump(Json::Value& value) const {
  Json::Value items = Json::arrayValue;
  for (const auto& [id, store] : stores_) {
    Json::Value item = Json::objectValue;
    store->Dump(item);
    items.append(item);
  }

  value["disks"] = items;
  return true;
}

}  // namespace cache
}  // namespace dingofs
