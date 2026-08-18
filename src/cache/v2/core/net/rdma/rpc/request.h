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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_REQUEST_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_REQUEST_H_

#include <cstddef>
#include <span>
#include <string_view>

#include "cache/v2/core/net/body.h"
#include "cache/v2/core/net/rdma/op_awaiter.h"
#include "cache/v2/core/net/rdma/rpc/frame.h"
#include "cache/v2/core/net/rdma/verbs/buffer.h"
#include "cache/v2/core/net/request.h"
#include "cache/v2/core/net/types.h"
#include "cache/v2/core/reactor/coroutine.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

class RdmaConnection;

// What a handler sees over rdma; payload points into the receive buffer.
class RdmaRequest final : public Request {
 public:
  RdmaRequest(RdmaConnection* conn, const FrameView& frame)
      : Request(frame.header->opcode, frame.payload, frame.body_shape(),
                frame.body_bytes()),
        conn_(conn),
        regions_(frame.regions, frame.region_count),
        correlation_(frame.header->correlation) {}

  Future<Status> Reply(ReplyCode code, std::string_view payload) override;

  // RDMA READ / RDMA WRITE + reply frame; both walk every advertised region.
  Future<Status> FetchBody(BufferView dst) override;

  // One-sided access to a peer-published region.
  // Validates peer-supplied claims; fails the op, never the process.
  OpAwaiter WriteTo(verbs::LocalBuf local, size_t index = 0);
  OpAwaiter ReadFrom(verbs::LocalBuf local, size_t index = 0);

  bool alive() const override;
  std::span<const RegionDesc> regions() const { return regions_; }
  RdmaConnection& conn() { return *conn_; }

 protected:
  Future<Status> DoReplyWithBody(ReplyCode code, std::string_view payload,
                                 BufferView body) override;

 private:
  // kAccepted when `index` names an advertised region big enough for `local`.
  OpAwaiter::Rejection CheckRegion(const verbs::LocalBuf& local,
                                   size_t index) const;

  // Chains one op per region across `buffer`; `read` picks READ over WRITE.
  Future<Status> MoveBody(BufferView buffer, bool read);

  RdmaConnection* conn_;
  std::span<const RegionDesc> regions_;
  uint64_t correlation_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_RPC_REQUEST_H_
