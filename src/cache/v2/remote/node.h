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

#ifndef DINGOFS_CACHE_V2_REMOTE_NODE_H_
#define DINGOFS_CACHE_V2_REMOTE_NODE_H_

#include <cstdint>
#include <iosfwd>
#include <memory>

#include "cache/v2/common/block_handle.h"
#include "cache/v2/common/mds_client.h"
#include "cache/v2/common/status.h"
#include "cache/v2/core/memory/buffer_view.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "cache/v2/remote/circuit_breaker.h"
#include "cache/v2/remote/connection.h"
#include "cache/v2/utils/gate.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RemoteNode {
 public:
  explicit RemoteNode(CacheGroupMember member);
  ~RemoteNode();

  RemoteNode(const RemoteNode&) = delete;
  RemoteNode& operator=(const RemoteNode&) = delete;

  Future<Status> Start();
  Future<> Shutdown();

  Future<Status> Put(BlockHandle handle, BufferViews block, bool stage);
  Future<Status> Get(BlockHandle handle, uint64_t offset, uint32_t length,
                     char* buffer);
  Future<Status> Prefetch(BlockHandle handle);
  Future<Status> Delete(BlockHandle handle);

  const CacheGroupMember& member() const { return member_; }

 private:
  struct Attachment {
    BufferViews send;
    BufferView receive;
  };

  template <typename Method, typename Request>
  Future<Status> SendRequest(Method method, uint64_t key, Request request,
                             Attachment attachment);

  bool running_ = false;
  CacheGroupMember member_;
  NodeConnectionsUPtr connections_;
  CircuitBreakerUPtr breaker_;
  Gate gate_;
};

using RemoteNodeUPtr = std::unique_ptr<RemoteNode>;

std::ostream& operator<<(std::ostream& os, const RemoteNode& node);

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_REMOTE_NODE_H_
