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

#include "cache/v2/tier/tier_cache.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>
#include <utility>

#include "cache/v2/block/local_cache.h"
#include "cache/v2/common/flag_decls.h"
#include "cache/v2/core/runtime/smp.h"
#include "cache/v2/remote/remote_cache.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_bool(fill_group_cache, true,
            "also send a written block to the cache group");

static bool DiskOrNone(const char* /*name*/, const std::string& value) {
  return value == "disk" || value == "none";
}

DEFINE_string(cache_store, "disk", "local disk cache: disk | none");
DEFINE_validator(cache_store, DiskOrNone);

TierCache::TierCache(ObjectStorage* storage, MDSClient* mds_client)
    : TierCache(storage, MakeLocal(storage), MakeRemote(mds_client)) {}

TierCache::TierCache(ObjectStorage* storage, BlockCacheUPtr local,
                     BlockCacheUPtr remote)
    : local_cache_(std::move(local)),
      remote_cache_(std::move(remote)),
      storage_(storage) {
  CHECK(storage_ != nullptr) << "TierCache requires an object storage";
}

TierCache::~TierCache() {
  LOG_IF(WARNING, running_) << "TierCache destroyed without Shutdown()";
}

Future<> TierCache::Start() {
  if (running_) {
    co_return;
  }

  LOG(INFO) << "TierCache{shard=" << ThisShardId() << "} is starting...";

  if (HasLocal()) {
    co_await local_cache_->Start();
  }

  if (HasRemote()) {
    co_await remote_cache_->Start();
  }

  running_ = true;

  LOG(INFO) << "Successfully start TierCache{shard=" << ThisShardId() << "}";
}

Future<> TierCache::Shutdown() {
  if (!running_) {
    co_return;
  }

  running_ = false;

  LOG(INFO) << "TierCache{shard=" << ThisShardId() << "} is shutting down...";

  if (HasRemote()) {
    co_await remote_cache_->Shutdown();
  }

  if (HasLocal()) {
    co_await local_cache_->Shutdown();
  }

  LOG(INFO) << "Successfully shutdown TierCache{shard=" << ThisShardId() << "}";
}

Future<Status> TierCache::Put(BlockHandle handle, BufferViews block,
                              PutOption option) {
  if (option.stage) {
    Status status = Status::NotFound("no cache tier can stage");

    if (HasLocal()) {
      status = co_await local_cache_->Put(handle, block, {.stage = true});
    }

    if (!status.ok() && HasRemote()) {
      status = co_await remote_cache_->Put(handle, block, {.stage = true});
    }

    if (status.ok()) {
      co_return status;
    }

    LOG(WARNING) << "Fail to stage " << handle
                 << ", writing through instead: " << status.ToString();
  }

  Future<Status> filling = MakeReadyFuture<Status>(Status::OK());
  if (HasRemote() && FLAGS_fill_group_cache) {
    filling = remote_cache_->Put(handle, block, {.stage = false});
  }
  const Status written = co_await storage_->Put(handle, block);
  const Status filled = co_await std::move(filling);  // TODO: fire and forget
  if (!filled.ok()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to fill the cache group with " << handle
                            << ": " << filled.ToString();
  }
  co_return written;
}

Future<Status> TierCache::Get(BlockHandle handle, uint64_t offset,
                              uint32_t length, char* buffer, GetOption option) {
  if (HasLocal()) {
    const Status status = co_await local_cache_->Get(
        handle, offset, length, buffer,
        {.retrieve_storage = false, .stats = option.stats});
    if (status.ok()) {
      co_return status;
    }
    LogTierMiss("local", handle, status);
  }

  if (HasRemote()) {
    const Status status = co_await remote_cache_->Get(
        handle, offset, length, buffer, {.stats = option.stats});
    if (status.ok()) {
      co_return status;
    }
    LogTierMiss("remote", handle, status);
  }

  if (!option.retrieve_storage) {
    co_return Status::NotFound("block is not cached");
  }

  // if (HasLocal()) {
  //   co_return co_await local_cache_->Get(handle, offset, length, buffer,
  //                                        {.retrieve_storage = true});
  // }
  co_return co_await storage_->Get(handle, offset, length, buffer,
                                   {.retry_notfound = true});
}

Future<Status> TierCache::Prefetch(BlockHandle handle, PrefetchOption option) {
  if (HasLocal()) {
    co_return co_await local_cache_->Prefetch(handle, option);
  }

  if (HasRemote()) {
    co_return co_await remote_cache_->Prefetch(handle, option);
  }

  co_return Status::NotSupport("there is no cache tier to prefetch into");
}

Future<Status> TierCache::Delete(BlockHandle handle, DeleteOption option) {
  Status result = Status::OK();
  if (HasLocal()) {
    result = co_await local_cache_->Delete(handle, option);
  }

  if (HasRemote()) {
    const Status status = co_await remote_cache_->Delete(handle, option);
    if (result.ok()) {
      result = status;
    }
  }
  co_return result;
}

Future<CacheStats> TierCache::GetStats() {
  CacheStats stats;
  if (HasLocal()) {
    stats = co_await local_cache_->GetStats();
  }

  if (HasRemote()) {
    const CacheStats from_remote = co_await remote_cache_->GetStats();
    stats.hits += from_remote.hits;
    stats.misses += from_remote.misses;
  }

  co_return stats;
}

BlockCacheUPtr TierCache::MakeLocal(ObjectStorage* storage) {
  if (FLAGS_cache_store != "disk") {
    LOG_IF(ERROR, FLAGS_cache_store != "none")
        << "--cache_store is disk | none, not " << FLAGS_cache_store
        << "; running without a local cache";
    return nullptr;
  }
  return std::make_unique<LocalCache>(storage);
}

BlockCacheUPtr TierCache::MakeRemote(MDSClient* mds_client) {
  if (FLAGS_cache_group.empty()) {
    return nullptr;
  }
  return std::make_unique<RemoteCache>(mds_client);
}

void TierCache::LogTierMiss(const char* tier, const BlockHandle& handle,
                            const Status& status) {
  if (status.IsNotFound()) {
    return;
  }
  if (status.IsCacheUnhealthy() || status.IsCacheDown()) {
    LOG_EVERY_SECOND(WARNING)
        << "The " << tier
        << " cache tier is not serving: " << status.ToString();
    return;
  }
  LOG(ERROR) << "Fail to read from the " << tier << " cache tier, " << handle
             << ": " << status.ToString();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
