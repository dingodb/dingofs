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

#ifndef DINGOFS_BLOCKCACHE_REMOTE_CONNECTION_H_
#define DINGOFS_BLOCKCACHE_REMOTE_CONNECTION_H_

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "blockcache/common/mds_client.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/channel.h"
#include "blockcache/net/types.h"
#include "blockcache/remote/stub.h"
#include "blockcache/utils/sync.h"

namespace dingofs {
namespace blockcache {

class NodeProber {
 public:
  NodeProber(std::string node_id, std::string address);

  Future<Status> Start();
  Future<> Shutdown();

  bool use_rdma() const { return use_rdma_; }
  uint32_t remote_shard_count() const { return remote_shard_count_; }
  const auto& remote_shards() const { return remote_shard_range_; }

 private:
  struct RemoteShardRange {
    unsigned base = 0;
    unsigned count = 1;

    bool Contains(unsigned remote_shard) const {
      return remote_shard - base < count;
    }
  };

  Future<Status> GetNodeInfo();

  static RemoteShardRange GetRemoteShardRange(unsigned local_shard_id,
                                              unsigned local_shards,
                                              unsigned remote_shards);
  static uint64_t FirstHintOf(unsigned shard, unsigned shards);

  std::string node_id_;
  std::string address_;
  SingleFlight probing_;
  bool ready_ = false;
  bool use_rdma_ = false;
  uint32_t remote_shard_count_ = 1;
  RemoteShardRange remote_shard_range_;
};

using NodeProberUPtr = std::unique_ptr<NodeProber>;

class NodeConnection {
 public:
  struct Option {
    std::string node_id;
    std::string server;
    unsigned remote_shard = 0;
    unsigned remote_shard_count = 1;
    bool use_rdma = false;
  };

  explicit NodeConnection(Option option);

  NodeConnection(const NodeConnection&) = delete;
  NodeConnection& operator=(const NodeConnection&) = delete;

  Future<Status> Open();
  Future<> Close();

  bool IsConnected() const;
  CacheStub* stub() const { return stub_.get(); }

 private:
  Future<Status> Establish();

  Option option_;
  SingleFlight opening_;
  ChannelUPtr channel_;
  CacheStubUPtr stub_;
};

using NodeConnectionUPtr = std::unique_ptr<NodeConnection>;

class NodeConnections {
 public:
  explicit NodeConnections(CacheGroupMember member);
  ~NodeConnections();

  NodeConnections(const NodeConnections&) = delete;
  NodeConnections& operator=(const NodeConnections&) = delete;

  Future<Status> Start();
  Future<> Shutdown();

  Future<StatusOr<CacheStub*>> Get(uint64_t key);

 private:
  void NewConnections();
  Future<Status> OpenAll();
  Future<> CloseAll();

  NodeConnection& GetConnection(uint64_t key);
  Future<StatusOr<CacheStub*>> DialThenGet(uint64_t key);

  std::string Address() const;

  friend std::ostream& operator<<(std::ostream& os,
                                  const NodeConnections& connections);

  bool running_ = false;
  CacheGroupMember member_;
  NodeProberUPtr prober_;
  std::vector<NodeConnectionUPtr> connections_;
};

using NodeConnectionsUPtr = std::unique_ptr<NodeConnections>;

std::ostream& operator<<(std::ostream& os, const NodeConnections& connections);

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_REMOTE_CONNECTION_H_
