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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_CONNECTION_TABLE_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_CONNECTION_TABLE_H_

#include <cstddef>
#include <memory>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/rdma/connection.h"

namespace dingofs {
namespace blockcache {

// Every connection one domain owns, and the one correct teardown.
class ConnectionTable {
 public:
  ConnectionTable() = default;

  ConnectionTable(const ConnectionTable&) = delete;
  ConnectionTable& operator=(const ConnectionTable&) = delete;

  // Takes ownership; false once closing began -- caller tears it down itself.
  bool Add(std::unique_ptr<RdmaConnection> conn);

  // Shuts down and drops connections whose peer is gone; returns how many.
  // Suspends, so `reaping_` stops a second reaper mid-walk.
  Future<size_t> ReapDead();

  // Marks accepted conns silent since `deadline_ns` dead; sync, no teardown.
  void MarkIdleDead(uint64_t deadline_ns);

  // Shuts every connection down and empties the table; `gate` closes between.
  // Order matters: shut down BEFORE closing the gate (else deadlock).
  // Loops are BY INDEX: Shutdown() suspends and Register() can still append.
  Future<> ShutdownAll(Gate* gate);

  size_t size() const { return conns_.size(); }
  bool empty() const { return conns_.empty(); }
  bool closing() const { return closing_; }

 private:
  std::vector<std::unique_ptr<RdmaConnection>> conns_;
  bool closing_ = false;
  bool reaping_ = false;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_CONNECTION_TABLE_H_
