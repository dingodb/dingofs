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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_OP_AWAITER_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_OP_AWAITER_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "blockcache/core/reactor/io_awaiter.h"
#include "blockcache/net/infiniband/buffer.h"
#include "blockcache/net/rdma/wr_id.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

class SendQueue;

// One RDMA operation, awaited directly: co_await conn.Write(local, remote).
// The awaiter IS the completion object: its address is the wr_id.
class OpAwaiter final : public IoAwaiter<OpAwaiter> {
 public:
  // Negative so it cannot collide with ibv_wc_status (always >= 0).
  enum Rejection : int32_t {
    kAccepted = 0,        // not a rejection: the arguments are usable
    kNoSuchRegion = -1,   // the peer advertised fewer regions than that
    kRegionTooSmall = -2  // the peer's region cannot hold this much
  };

  OpAwaiter(SendQueue* queue, ibv_wr_opcode opcode, verbs::LocalBuf local,
            uint64_t remote_addr, uint32_t rkey) noexcept
      : queue_(queue),
        local_(local),
        remote_addr_(remote_addr),
        rkey_(rkey),
        opcode_(opcode) {}

  explicit OpAwaiter(Rejection why) noexcept { result_ = why; }

  // A rejected operation never suspends; Arm() is never called.
  bool await_ready() const noexcept { return queue_ == nullptr; }

  // OK, the rejection reason, or the failing work completion status.
  Status await_resume() const;

  void Arm();

  // Called by the CQ poller through the wr_id tag.
  void Complete(int32_t wc_status) noexcept { ResumeLater(wc_status); }

  bool IsRead() const { return opcode_ == IBV_WR_RDMA_READ; }
  SendQueue* queue() const { return queue_; }

  // Intrusive FIFO hook, owned by the send queue while parked.
  OpAwaiter* park_next = nullptr;

 private:
  friend class SendQueue;

  // Null for a rejected operation, which is what await_ready() tests.
  SendQueue* queue_ = nullptr;
  verbs::LocalBuf local_;
  uint64_t remote_addr_ = 0;
  uint32_t rkey_ = 0;
  ibv_wr_opcode opcode_ = IBV_WR_RDMA_WRITE;
};

// Tail sentinel: only the last WR is signalled; RC ordering proves the rest.
class BatchAwaiter final : public IoAwaiter<BatchAwaiter> {
 public:
  BatchAwaiter(SendQueue* queue, ibv_send_wr* tail, uint32_t covered,
               uint32_t reads_covered) noexcept
      : queue_(queue),
        tail_(tail),
        covered_(covered),
        reads_covered_(reads_covered) {}

  // An empty batch has nothing to wait for.
  bool await_ready() const noexcept { return covered_ == 0; }
  Status await_resume() const;

  void Arm();

  // Called by the CQ poller through the wr_id tag.
  void Complete(int32_t wc_status) noexcept { ResumeLater(wc_status); }

  SendQueue* queue() const { return queue_; }

 private:
  friend class SendQueue;

  SendQueue* queue_;
  ibv_send_wr* tail_;
  uint32_t covered_;
  uint32_t reads_covered_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_OP_AWAITER_H_
