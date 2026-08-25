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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_BASE_QUEUE_PAIR_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_BASE_QUEUE_PAIR_H_

#include <infiniband/verbs.h>

#include <cstdint>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/device.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

struct QueuePairOption {
  uint32_t max_send_wr = 512;
  uint32_t max_recv_wr = 512;
  uint32_t max_send_sge = 1;  // one message, one contiguous send buffer
  uint32_t max_recv_sge = 1;
  uint32_t max_inline_data = 192;
  uint8_t timeout = 14;  // ~67ms per attempt
  uint8_t retry_cnt = 7;
  uint8_t rnr_retry = 3;
  uint8_t min_rnr_timer = 12;  // ~64us
};

struct QueuePairInfo {
  uint32_t qpn;
  uint32_t psn;
  uint16_t lid;
  uint8_t port_num;
  uint8_t mtu;
  uint8_t gid[16];
};

class QueuePair {
 public:
  static constexpr uint8_t kMaxRdAtomic = 16;

  static StatusOr<QueuePair> Create(Device& device, ibv_cq* cq,
                                    const QueuePairOption& option);

  QueuePair() = default;
  ~QueuePair() { Reset(); }

  QueuePair(const QueuePair&) = delete;
  QueuePair& operator=(const QueuePair&) = delete;

  QueuePair(QueuePair&& o) noexcept
      : device_(o.device_),
        qp_(o.qp_),
        option_(o.option_),
        start_psn_(o.start_psn_) {
    o.qp_ = nullptr;
  }
  QueuePair& operator=(QueuePair&& o) noexcept {
    if (this != &o) {
      Reset();
      device_ = o.device_;
      qp_ = o.qp_;
      option_ = o.option_;
      start_psn_ = o.start_psn_;
      o.qp_ = nullptr;
    }
    return *this;
  }

  Status ModifyToInit();
  Status ModifyToRtr(const QueuePairInfo& remote);
  Status ModifyToRts();
  void ModifyToError() noexcept;

  QueuePairInfo GetInfo() const;

  ibv_qp* get() const { return qp_; }
  uint32_t max_inline_data() const { return option_.max_inline_data; }

 private:
  QueuePair(Device* device, ibv_qp* qp, const QueuePairOption& option);

  void Reset() noexcept;

  Device* device_ = nullptr;
  ibv_qp* qp_ = nullptr;
  QueuePairOption option_;
  uint32_t start_psn_ = 0;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_BASE_QUEUE_PAIR_H_
