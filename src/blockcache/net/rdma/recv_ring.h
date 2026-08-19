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

#ifndef DINGOFS_BLOCKCACHE_NET_RDMA_RECV_RING_H_
#define DINGOFS_BLOCKCACHE_NET_RDMA_RECV_RING_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <vector>

#include "blockcache/core/memory/slab_pool.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

class RdmaConnection;

// One receive buffer; carries its connection for wr-id routing.
struct alignas(8) RecvSlot {
  RdmaConnection* conn = nullptr;
  char* data = nullptr;
  uint16_t idx = 0;
};

// Receive side of the small lane; one lkey, batched replenishment.
class RecvRing {
 public:
  RecvRing() = default;
  ~RecvRing();

  RecvRing(const RecvRing&) = delete;
  RecvRing& operator=(const RecvRing&) = delete;

  // Buffers carved from `pool` (covered by `lkey`); kept until destruction.
  Status Init(ibv_qp* qp, RdmaConnection* conn, SlabPool* pool, uint32_t lkey,
              uint16_t count, uint32_t buffer_bytes);

  // Must run while the queue pair is still in INIT.
  void PostAll();

  // The message in `idx` was consumed; the buffer re-posts in batches.
  void Recycle(uint16_t recv_slot);
  void FlushReplenish();

  void OnWcArrived() { --outstanding_; }

  // After the QP is in ERROR: keep reclaiming completions, stop posting.
  void SetDead() { dead_ = true; }

  uint32_t outstanding() const { return outstanding_; }
  uint16_t count() const { return static_cast<uint16_t>(slots_.size()); }

 private:
  static constexpr uint32_t kReplenishBatch = 32;

  void PostSlots(const uint16_t* indices, uint32_t n);

  ibv_qp* qp_ = nullptr;
  SlabPool* pool_ = nullptr;  // slots_[].data goes back here
  uint32_t lkey_ = 0;
  uint32_t buffer_bytes_ = 0;
  uint32_t outstanding_ = 0;
  bool dead_ = false;

  std::vector<RecvSlot> slots_;
  std::vector<uint16_t> pending_;
  std::vector<ibv_recv_wr> wrs_;
  std::vector<ibv_sge> sges_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_RDMA_RECV_RING_H_
