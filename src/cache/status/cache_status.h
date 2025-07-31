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

#ifndef DINGOFS_SRC_CACHE_DEBUG_EXPORTER_H_
#define DINGOFS_SRC_CACHE_DEBUG_EXPORTER_H_

#include <brpc/server.h>

#include "cache/common/proto.h"
#include "cache/common/type.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace cache {

class CacheServiceImpl final : public PBCacheService {
 public:
  void default_method(google::protobuf::RpcController* controller,
                      const pb::cache::DebugCacheRequest* request,
                      pb::cache::DebugCacheResponse* response,
                      google::protobuf::Closure* done) override;
};

Status AddCacheService(brpc::Server* server);

class CacheStatus {
 public:
  struct Disk {
    std::string dir;
    size_t capacity;
    std::string health;
  };

  struct Node {
    uint64_t id;
    std::string address;
    uint32_t weight;
    std::string state;  // online, offline
    std::string health;
  };

  struct Property {
    // std::string cache_store{"none"};
    bool enable_stage{false};
    bool enable_cache{false};
  };

  struct LocalCache {
    Property property;
    std::unordered_map<uint32_t, Disk> disks;
  };

  struct RemoteCache {
    Property property;
    std::string mds_addrs;
    std::string cache_group;
    std::string last_modified;
    std::unordered_map<uint64_t, Node> nodes;
  };

  struct Root {
    LocalCache local_cache;
    RemoteCache remote_cache;
  };

  using UpdateFunc = std::function<void(Root& root)>;
  static void Update(UpdateFunc update_func);
  static std::string Dump();

 private:
  static nlohmann::json ToJSON(Disk disk);
  static nlohmann::json ToJSON(Node node);
  static nlohmann::json ToJSON(Property property);
  static nlohmann::json ToJSON(LocalCache cache);
  static nlohmann::json ToJSON(RemoteCache cache);
  static nlohmann::json ToJSON(Root root);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_DEBUG_EXPORTER_H_
