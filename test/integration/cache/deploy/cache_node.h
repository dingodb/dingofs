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

/*
 * Project: DingoFS
 * Created Date: 2026-06-22
 * Author: AI
 */

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_CACHE_NODE_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_CACHE_NODE_H_

#include <cstdint>
#include <string>

#include "common/status.h"
#include "test/integration/cache/deploy/process_guard.h"

namespace dingofs {
namespace cache {
namespace integration {

// Spins up a real dingo-cache process (disk store, TCP transport). The node
// registers itself into the given cache group on the MDS; callers verify the
// registration via MdsServer/MDSClient ListMembers. Requires an active RDMA
// device (the on-disk cache pins its O_DIRECT buffers in the RDMA slab pool);
// the device name is forwarded from the test process' FLAGS_cache_rdma_device.
class CacheNode {
 public:
  CacheNode() = default;
  ~CacheNode() { Stop(); }

  CacheNode(const CacheNode&) = delete;
  CacheNode& operator=(const CacheNode&) = delete;

  // Spawns dingo-cache on a free port with a private cache dir under `workdir`,
  // and waits until its RPC port accepts connections (NOT until it registers --
  // the caller checks registration through the MDS). `cache_size_mb` bounds the
  // node's on-disk cache budget (lower it to drive node-side eviction).
  // `fixed_port` (non-zero) pins the listen port instead of picking a free one;
  // restarting a node on its previous port lets it rejoin under the same id
  // (the MDS rejects a relocated, still-locked member with ELOCKED).
  Status Start(const std::string& workdir, const std::string& mds_addr,
               const std::string& group, const std::string& member_id,
               uint64_t cache_size_mb = 1024, int fixed_port = 0);

  void Stop() { proc_.Stop(); }
  bool Running() const { return proc_.Running(); }

  const std::string& Id() const { return id_; }
  std::string Ip() const { return "127.0.0.1"; }
  int Port() const { return port_; }

 private:
  ProcessGuard proc_;
  std::string id_;
  int port_{0};
};

// Sends a BlockCacheService.Ping RPC to a cache node; true if it replies OK.
bool PingNode(const std::string& ip, int port, int timeout_ms = 3000);

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_CACHE_NODE_H_
