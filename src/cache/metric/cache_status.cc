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
 * Created Date: 2025-07-27
 * Author: Jingli Chen (Wine93)
 */

#include "cache/metric/cache_status.h"

#include <brpc/server.h>

#include "cache/common/type.h"

namespace dingofs {
namespace cache {

static CacheServiceImpl cache_service;

void CacheServiceImpl::default_method(
    google::protobuf::RpcController* controller,
    const pb::cache::DebugCacheRequest* /*request*/,
    pb::cache::DebugCacheResponse* /*response*/,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("application/json");

  butil::IOBufBuilder os;
  os << CacheStatus::Dump();
  os.move_to(cntl->response_attachment());
}

Status AddCacheService(brpc::Server* server) {
  int rc = server->AddService(&cache_service, brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Failed to add cache service.";
    return Status::Internal("add cache service failed");
  }
  return Status::OK();
}

static CacheStatus::Root root;
static BthreadRWLock rwlock;

void CacheStatus::Update(UpdateFunc update_func) {
  WriteLockGuard lock(rwlock);
  update_func(root);
}

std::string CacheStatus::Dump() {
  ReadLockGuard lock(rwlock);
  return ToJSON(root).dump();
}

nlohmann::json CacheStatus::ToJSON(Node node) {
  nlohmann::json msg;
  msg["id"] = node.id;
  msg["address"] = node.address;
  msg["weight"] = node.weight;
  msg["state"] = node.state;
  msg["health"] = node.health;
  return msg;
}

nlohmann::json CacheStatus::ToJSON(Disk disk) {
  nlohmann::json msg;
  msg["dir"] = disk.dir;
  msg["capacity"] = absl::StrFormat("%.2lf MiB", disk.capacity);
  msg["health"] = disk.health;
  return msg;
}

nlohmann::json CacheStatus::ToJSON(Property property) {
  nlohmann::json msg;
  msg["cache_store"] = property.cache_store;
  msg["enable_stage"] = property.enable_stage;
  msg["enable_cache"] = property.enable_cache;
  return msg;
}

nlohmann::json CacheStatus::ToJSON(LocalCache cache) {
  nlohmann::json msg;

  msg["property"] = ToJSON(cache.property);

  nlohmann::json disks;
  for (const auto& item : cache.disks) {
    disks.push_back(ToJSON(item.second));
  }
  msg["disks"] = disks;

  return msg;
}

nlohmann::json CacheStatus::ToJSON(RemoteCache cache) {
  nlohmann::json msg;

  msg["mds_addrs"] = cache.mds_addrs;
  msg["cache_group"] = cache.cache_group;
  msg["last_modified"] = cache.last_modified;

  nlohmann::json nodes;
  for (const auto& item : cache.nodes) {
    nodes.push_back(ToJSON(item.second));
  }
  msg["nodes"] = nodes;

  return msg;
}

nlohmann::json CacheStatus::ToJSON(Root root) {
  nlohmann::json msg;
  msg["local_cache"] = ToJSON(root.local_cache);
  msg["remote_cache"] = ToJSON(root.remote_cache);
  return msg;
}

}  // namespace cache
}  // namespace dingofs
