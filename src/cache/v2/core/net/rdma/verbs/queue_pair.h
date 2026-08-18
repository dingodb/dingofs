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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_QUEUE_PAIR_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_QUEUE_PAIR_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/verbs/device.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

struct QpOption {
  uint32_t max_send_wr = 512;
  uint32_t max_recv_wr = 512;
  uint32_t max_send_sge = 2;  // frame header + payload, gathered in one WR
  uint32_t max_recv_sge = 1;
  // Copied into the WQE by the CPU; best effort, retried with 0 on refusal.
  uint32_t max_inline_data = 192;
  // 14 ~= 67ms per attempt; governs LOST packets, not flow control.
  uint8_t timeout = 14;
  uint8_t retry_cnt = 7;
  // Credits make RNR impossible; a small budget turns a bug into latency.
  uint8_t rnr_retry = 3;
  uint8_t min_rnr_timer = 12;  // ~64us
};

// Wire-format QP identity, memcpy'd into a handshake frame.
struct __attribute__((packed)) QpPeer {
  uint32_t qpn;
  uint32_t psn;
  uint16_t lid;
  uint8_t port_num;
  uint8_t link_layer;  // LinkLayer
  uint8_t mtu;         // ibv_mtu
  uint8_t rd_atomic;   // sender's max_qp_rd_atom; both sides take the min
  uint8_t reserved[2];
  uint8_t gid[16];
};
static_assert(sizeof(QpPeer) == 32, "QpPeer is a wire structure");

// Move-only RAII over one RC queue pair, plus the INIT -> RTR -> RTS ladder.
class QueuePair {
 public:
  // send_cq and recv_cq are the same CQ: one CQ per shard.
  static StatusOr<QueuePair> Create(Device& device, ibv_cq* cq,
                                    const QpOption& option);

  QueuePair() = default;
  ~QueuePair() { Reset(); }

  QueuePair(const QueuePair&) = delete;
  QueuePair& operator=(const QueuePair&) = delete;

  QueuePair(QueuePair&& o) noexcept : qp_(o.qp_), max_inline_(o.max_inline_) {
    o.qp_ = nullptr;
  }
  QueuePair& operator=(QueuePair&& o) noexcept {
    if (this != &o) {
      Reset();
      qp_ = o.qp_;
      max_inline_ = o.max_inline_;
      o.qp_ = nullptr;
    }
    return *this;
  }

  Status ToInit(uint8_t port_num, unsigned access = IBV_ACCESS_REMOTE_READ |
                                                    IBV_ACCESS_REMOTE_WRITE);
  Status ToRtr(const QpPeer& remote, const PortInfo& local, uint8_t rd_atomic);
  Status ToRts(uint32_t local_psn, uint8_t rd_atomic, const QpOption& option);
  void ToError() noexcept;

  // Fills the wire descriptor a peer needs to reach this QP.
  void FillPeer(const PortInfo& local, uint32_t psn, uint8_t rd_atomic,
                QpPeer* out) const;

  bool Valid() const { return qp_ != nullptr; }
  ibv_qp* get() const { return qp_; }
  uint32_t qpn() const { return qp_->qp_num; }
  // What the driver actually granted, which may be less than requested.
  uint32_t max_inline() const { return max_inline_; }

  // Read-back helpers for tests and assertions.
  uint8_t QueryRdAtomic() const;
  ibv_qp_state QueryState() const;

 private:
  explicit QueuePair(ibv_qp* qp, uint32_t max_inline)
      : qp_(qp), max_inline_(max_inline) {}
  void Reset() noexcept;

  ibv_qp* qp_ = nullptr;
  uint32_t max_inline_ = 0;
};

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_QUEUE_PAIR_H_
