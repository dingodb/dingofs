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

#include "cache/debug/expose.h"

#include <absl/strings/strip.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>

#include "cache/common/proto.h"
#include "cache/common/type.h"
#include "cache/utils/helper.h"
#include "dingofs/cachegroup.pb.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace cache {

struct Disk {
  std::string dir;
  size_t capacity;
  std::string health;
};

struct Node {
  uint64_t id;
  std::string address;
  uint32_t weight;
  std::string state;   // online, unstable
  std::string health;  // normal, unstable, down
};

struct LocalCache {
  std::unordered_map<uint32_t, Disk> disks;
};

struct RemoteCache {
  std::string mds_addrs;
  std::string cache_group;
  std::string last_modified;
  std::unordered_map<uint64_t, Node> nodes;
};

struct Root {
  LocalCache local_cache;
  RemoteCache remote_cache;
};

static nlohmann::json ToJSON(Node node) {
  nlohmann::json msg;
  msg["id"] = node.id;
  msg["address"] = node.address;
  msg["weight"] = node.weight;
  msg["state"] = node.state;
  msg["health"] = node.health;
  return msg;
}

static nlohmann::json ToJSON(Disk disk) {
  nlohmann::json msg;
  msg["dir"] = disk.dir;
  msg["capacity"] = absl::StrFormat("%.2lf MiB", disk.capacity);
  msg["health"] = disk.health;
  return msg;
}

static nlohmann::json ToJSON(LocalCache cache) {
  nlohmann::json msg;

  nlohmann::json disks;
  for (const auto& item : cache.disks) {
    disks.push_back(ToJSON(item.second));
  }
  msg["disks"] = disks;

  return msg;
}

static nlohmann::json ToJSON(RemoteCache cache) {
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

static nlohmann::json ToJSON(Root root) {
  nlohmann::json msg;
  msg["local_cache"] = ToJSON(root.local_cache);
  msg["remote_cache"] = ToJSON(root.remote_cache);
  return msg;
}

static Root root;
static CacheServiceImpl cache_service;
static BthreadRWLock rwlock;

void CacheServiceImpl::default_method(
    google::protobuf::RpcController* controller,
    const pb::cache::DebugCacheRequest* /*request*/,
    pb::cache::DebugCacheResponse* /*response*/,
    google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(controller);
  cntl->http_response().set_content_type("application/json");

  butil::IOBufBuilder os;
  {
    ReadLockGuard lk(rwlock);
    os << ToJSON(root).dump();
  }
  os.move_to(cntl->response_attachment());
}

Status AddCacheService(brpc::Server* server) {
  int rc = server->AddService(&cache_service, brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    LOG(ERROR) << "Failed to add cache service";
    return Status::Internal("add cache service failed");
  }
  return Status::OK();
}

void ExposeDiskCaches(std::vector<DiskCacheOption> options) {
  WriteLockGuard lk(rwlock);
  auto& disks = root.local_cache.disks;
  for (int i = 0; i < options.size(); i++) {
    disks[i] = Disk{
        .dir = options[i].cache_dir,
        .capacity = options[i].cache_size_mb,
        .health = "unknown",
    };
  }
}

void ExposeDiskCacheHealth(uint32_t cache_index, const std::string& health) {
  WriteLockGuard lk(rwlock);
  root.local_cache.disks[cache_index].health = health;
}

void ExposeMDSAddrs(const std::vector<std::string>& addrs) {
  WriteLockGuard lk(rwlock);
  root.remote_cache.mds_addrs = absl::StrJoin(addrs, ", ");
}

void ExposeCacheGroupName(const std::string& cache_group) {
  WriteLockGuard lk(rwlock);
  root.remote_cache.cache_group = cache_group;
}

void ExposeRemoteCacheNodes(const PBCacheGroupMembers& members) {
  WriteLockGuard lk(rwlock);
  auto& nodes = root.remote_cache.nodes;
  nodes.clear();
  for (const auto& member : members) {
    if (member.state() ==
        PBCacheGroupMemberState::CacheGroupMemberStateOffline) {
      continue;
    }

    nodes[member.id()] = Node{
        .id = member.id(),
        .address = absl::StrFormat("%s:%d", member.ip(), member.port()),
        .weight = member.weight(),
        .state = pb::mds::cachegroup::CacheGroupMemberState_Name(member.state())
                     .substr(21),
        .health = "unknown",
    };
  }
  root.remote_cache.last_modified = Helper::NowTime();
}

void ExposeRemoteCacheNodeHealth(uint64_t id, const std::string& health) {
  WriteLockGuard lk(rwlock);
  auto& nodes = root.remote_cache.nodes;
  nodes[id].health = health;
}

}  // namespace cache
}  // namespace dingofs
