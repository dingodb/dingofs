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

#include "blockcache/net/rdma/connection_table.h"

#include <cerrno>
#include <utility>
#include <vector>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {

bool ConnectionTable::Add(std::unique_ptr<RdmaConnection> conn) {
  if (closing_) {
    return false;
  }
  conns_.push_back(std::move(conn));
  return true;
}

Future<size_t> ConnectionTable::ReapDead() {
  if (closing_ || reaping_) {
    co_return 0;
  }
  reaping_ = true;

  // By index: Shutdown() suspends and Register() can still append.
  // NOLINTNEXTLINE(modernize-loop-convert) -- a range-for would walk a vector
  // that reallocated underneath it.
  for (size_t i = 0; i < conns_.size(); ++i) {
    if (!conns_[i]->alive()) {
      co_await conns_[i]->Shutdown();
    }
  }

  // Erased in one pass afterwards so no index is invalidated by suspension.
  const size_t reaped =
      std::erase_if(conns_, [](const std::unique_ptr<RdmaConnection>& conn) {
        return conn->shutdown_done();
      });
  reaping_ = false;
  co_return reaped;
}

void ConnectionTable::MarkIdleDead(uint64_t deadline_ns) {
  for (const std::unique_ptr<RdmaConnection>& conn : conns_) {
    if (conn->alive() && !conn->initiator() &&
        conn->last_heard_ns() < deadline_ns) {
      conn->OnError(ToStatus(ETIMEDOUT, "hear from the peer"));
    }
  }
}

Future<> ConnectionTable::ShutdownAll(Gate* gate) {
  closing_ = true;
  // NOLINTNEXTLINE(modernize-loop-convert) -- see the header's rule (2): a
  // range-for here is the use-after-free this class exists to prevent.
  for (size_t i = 0; i < conns_.size(); ++i) {
    co_await conns_[i]->Shutdown();
  }
  co_await gate->Close();
  // A handshake that landed during the pass above.
  // NOLINTNEXTLINE(modernize-loop-convert) -- same reason.
  for (size_t i = 0; i < conns_.size(); ++i) {
    co_await conns_[i]->Shutdown();
  }
  conns_.clear();
}

}  // namespace blockcache
}  // namespace dingofs
