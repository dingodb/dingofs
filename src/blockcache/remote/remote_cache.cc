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

#include "blockcache/remote/remote_cache.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/infiniband/client/context.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {

DEFINE_string(cache_group, "", "cache group; empty disables it");

DEFINE_bool(remote_rdma, false, "use rdma");
DEFINE_string(remote_rdma_device, "", "rdma device; empty picks the first");

DEFINE_uint32(remote_rpc_timeout_ms, 30000, "rpc timeout");
DEFINE_validator(remote_rpc_timeout_ms, brpc::PassValidate);
DEFINE_uint32(remote_connect_timeout_ms, 1000, "connect timeout");
DEFINE_validator(remote_connect_timeout_ms, brpc::PassValidate);

RemoteCache::RemoteCache(MDSClient* mds_client)
    : mds_client_(mds_client), nodes_(std::make_unique<RemoteNodeGroup>()) {
  CHECK(!FLAGS_cache_group.empty()) << "a remote cache needs a cache group";
}

RemoteCache::~RemoteCache() {
  LOG_IF(WARNING, running_) << "RemoteCache destroyed without Shutdown()";
}

Future<> RemoteCache::Start() {
  if (running_) {
    co_return;
  }

  LOG(INFO) << "RemoteCache{shard=" << ThisShardId() << "} is starting...";

  InitInfiniband();
  co_await nodes_->Start();

  running_ = true;

  StartSyncer();
  co_await WaitForMembersSynced();

  LOG(INFO) << "Successfully start RemoteCache{shard=" << ThisShardId() << "}";
}

Future<> RemoteCache::Shutdown() {
  if (!running_) {
    co_return;
  }

  running_ = false;

  LOG(INFO) << "RemoteCache{shard=" << ThisShardId() << "} is shutting down...";

  ShutdownSyncer();
  co_await nodes_->Shutdown();
  co_await ShutdownInfiniband();

  LOG(INFO) << "Successfully shutdown RemoteCache{shard=" << ThisShardId()
            << "}";
}

void RemoteCache::InitInfiniband() {
  if (!FLAGS_remote_rdma) {
    return;
  }
  const Status status =
      infiniband::InfinibandContext::Create(FLAGS_remote_rdma_device);
  LOG_IF(FATAL, !status.ok()) << "Fail to open the rdma context on shard "
                              << ThisShardId() << ": " << status.ToString();
}

Future<> RemoteCache::ShutdownInfiniband() {
  infiniband::InfinibandContext* context = infiniband::ThisInfinibandContext();
  if (context == nullptr) {
    co_return;
  }
  co_await context->poller->Disarm();
  infiniband::tls_infiniband_context.reset();
}

void RemoteCache::StartSyncer() {
  if (ThisShardId() != 0) {
    return;
  }

  syncer_ = std::make_unique<CacheGroupMemberSyncer>(mds_client_);
  syncer_->Start();
}

void RemoteCache::ShutdownSyncer() {
  if (ThisShardId() != 0) {
    return;
  }

  syncer_->Shutdown();
  syncer_.reset();
}

Future<> RemoteCache::WaitForMembersSynced() {
  static constexpr uint64_t kFirstSyncWaitMs = 10000;
  co_await SleepWhile([this] { return nodes_->empty(); }, kFirstSyncWaitMs);
  LOG_IF(FATAL, nodes_->empty())
      << "Fail to sync member of cache group=" << FLAGS_cache_group
      << " from the mds in " << kFirstSyncWaitMs
      << " ms; is the mds alive and does the group have online members?";
}

Future<Status> RemoteCache::Put(BlockHandle handle, BufferViews body,
                                PutOption option) {
  StatusOr<RemoteNode*> node = nodes_->GetNode(handle.Hash());
  if (!node.ok()) {
    co_return node.status();
  }
  co_return co_await node.value()->Put(handle, body, option.stage);
}

Future<Status> RemoteCache::Get(BlockHandle handle, uint64_t offset,
                                uint32_t length, char* buffer,
                                GetOption option) {
  StatusOr<RemoteNode*> node = nodes_->GetNode(handle.Hash());
  if (!node.ok()) {
    co_return node.status();
  }
  const Status status =
      co_await node.value()->Get(handle, offset, length, buffer);
  if (status.ok()) {
    ++hits_;
    if (option.stats != nullptr) {
      option.stats->hit = true;
    }
  } else {
    ++misses_;
  }
  co_return status;
}

Future<Status> RemoteCache::Prefetch(BlockHandle handle, PrefetchOption) {
  StatusOr<RemoteNode*> node = nodes_->GetNode(handle.Hash());
  if (!node.ok()) {
    co_return node.status();
  }
  co_return co_await node.value()->Prefetch(handle);
}

Future<Status> RemoteCache::Delete(BlockHandle handle, DeleteOption) {
  StatusOr<RemoteNode*> node = nodes_->GetNode(handle.Hash());
  if (!node.ok()) {
    co_return node.status();
  }
  co_return co_await node.value()->Delete(handle);
}

Future<CacheStats> RemoteCache::GetStats() {
  return MakeReadyFuture<CacheStats>(
      CacheStats{.hits = hits_, .misses = misses_});
}

}  // namespace blockcache
}  // namespace dingofs
