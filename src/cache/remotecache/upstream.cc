/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-01-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/upstream.h"

#include <butil/logging.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "cache/common/mds_client.h"
#include "cache/remotecache/peer_group.h"
#include "common/options/cache.h"
#include "dingofs/blockcache.pb.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_uint32(periodic_sync_members_ms, 3000,
              "periodic sync members from mds in milliseconds");

Upstream::Upstream()
    : running_(false),
      mds_client_(std::make_unique<MDSClientImpl>()),
      executor_(std::make_unique<BthreadExecutor>()),
      group_(std::make_shared<PeerGroup>()),
      builder_(std::make_unique<PeerGroupBuilder>()),
      vars_(std::make_unique<UpstreamVarsCollector>()) {}

void Upstream::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Upstream already started";
    return;
  }

  LOG(INFO) << "Upstream is starting...";

  if (!SyncMembers()) {
    LOG(FATAL)
        << "Fail to sync members from mds, is there any member in cache group="
        << FLAGS_cache_group << "?";
    return;
  }

  CHECK(executor_->Start());
  executor_->Schedule([this] { PeriodicSyncMembers(); },
                      FLAGS_periodic_sync_members_ms);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "Upstream{mds_addr=" << FLAGS_mds_addrs << "} started";
}

void Upstream::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Upstream already shutdown";
    return;
  }

  LOG(INFO) << "Upstream is shutting down...";

  CHECK(executor_->Stop());

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "Upstream shutdown";
}

Status Upstream::SendPutRequest(ContextSPtr /*ctx*/, const BlockKey& key,
                                const Block& block) {
  Status status;
  UpstreamVarsRecordGuard guard("Put", block.size, status, vars_.get());

  pb::cache::PutRequest raw;
  *raw.mutable_block_key() = key.ToPB();
  raw.set_block_size(block.buffer.Size());
  auto request = MakeRequest("Put", raw, &block.buffer);

  auto response =
      SendRequest<pb::cache::PutRequest, pb::cache::PutResponse>(request);
  status = response.status;
  if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

Status Upstream::SendRangeRequest(ContextSPtr ctx, const BlockKey& key,
                                  off_t offset, size_t length, IOBuffer* buffer,
                                  size_t block_whole_length) {
  Status status;
  UpstreamVarsRecordGuard guard("Range", length, status, vars_.get());

  pb::cache::RangeRequest raw;
  *raw.mutable_block_key() = key.ToPB();
  raw.set_offset(offset);
  raw.set_length(length);
  raw.set_block_size(block_whole_length);
  auto request = MakeRequest("Range", raw);

  auto response =
      SendRequest<pb::cache::RangeRequest, pb::cache::RangeResponse>(request);
  status = response.status;
  if (status.ok()) {
    *buffer = std::move(response.body);
    ctx->SetCacheHit(response.raw.cache_hit());
  } else if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

Status Upstream::SendCacheRequest(ContextSPtr /*ctx*/, const BlockKey& key,
                                  const Block& block) {
  Status status;
  UpstreamVarsRecordGuard guard("Cache", block.size, status, vars_.get());

  pb::cache::CacheRequest raw;
  *raw.mutable_block_key() = key.ToPB();
  raw.set_block_size(block.buffer.Size());
  auto request = MakeRequest("Cache", raw, &block.buffer);

  auto response =
      SendRequest<pb::cache::CacheRequest, pb::cache::CacheResponse>(request);
  status = response.status;
  if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

Status Upstream::SendPrefetchRequest(ContextSPtr /*ctx*/, const BlockKey& key,
                                     size_t length) {
  Status status;
  UpstreamVarsRecordGuard guard("Prefetch", length, status, vars_.get());

  pb::cache::PrefetchRequest raw;
  *raw.mutable_block_key() = key.ToPB();
  raw.set_block_size(length);
  auto request = MakeRequest("Prefetch", raw);

  auto response =
      SendRequest<pb::cache::PrefetchRequest, pb::cache::PrefetchResponse>(
          request);
  status = response.status;
  if (status.IsCacheUnhealthy()) {
    LOG_EVERY_SECOND(ERROR) << "Fail to send " << request;
  } else if (!status.ok()) {
    LOG(ERROR) << "Fail to send " << request;
  }
  return status;
}

template <typename T, typename U>
Response<U> Upstream::SendRequest(const Request<T>& request) {
  // FIXME: blockkey
  auto peer_group = CHECK_NOTNULL(GetPeerGroup());
  auto peer =
      peer_group->SelectPeer(BlockKey(request.raw.block_key()).Filename());
  if (nullptr == peer) {
    LOG(ERROR) << "No peer found for " << request;
    return Response<U>{Status::NotFound("no peer found")};
  } else if (!peer->IsHealthy()) {
    LOG_EVERY_SECOND(WARNING)
        << "Fail to send request to " << peer << ", because "
        << "peer is unhealthy";
    return Response<U>{Status::CacheUnhealthy("peer is unhealthy")};
  }

  return peer->template SendRequest<T, U>(request);
}

bool Upstream::SendListMembersRequest(Members* members) {
  auto group_name = FLAGS_cache_group;
  auto status = mds_client_->ListMembers(group_name, members);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to list cache group=" << group_name
               << " members from mds";
    return false;
  } else if (members->empty()) {
    LOG(WARNING) << "No member found in cache group=" << group_name;
    return false;
  }
  return true;
}

bool Upstream::SyncMembers() {
  Members members;
  if (!SendListMembersRequest(&members)) {
    return false;
  }

  auto new_group = builder_->Build(members);
  if (new_group != nullptr) {
    SetPeerGroup(new_group);
    LOG(INFO) << "Successfully rebuild upstream peer group";
  }
  return true;
}

void Upstream::PeriodicSyncMembers() {
  SyncMembers();
  executor_->Schedule([this] { PeriodicSyncMembers(); },
                      FLAGS_periodic_sync_members_ms);
}

bool Upstream::Dump(Json::Value& value) {
  auto group = GetPeerGroup();
  if (nullptr == group) {
    return true;
  }

  Json::Value items = Json::arrayValue;
  for (const auto& [id, peer] : group->peers) {
    Json::Value item = Json::objectValue;
    peer->Dump(item);

    items.append(item);
  }

  value["members"] = items;
  return true;
}

}  // namespace cache
}  // namespace dingofs
