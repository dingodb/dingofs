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

#include "cache/v2/core/net/rdma/lane_set.h"

#include <glog/logging.h>

#include <cstring>
#include <random>
#include <utility>

#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/option.h"

namespace dingofs {
namespace cache {
namespace v2 {

verbs::QpOption SmallLaneOption(const RdmaOption& option) {
  verbs::QpOption qp;
  qp.max_send_wr = option.small_send_wr();
  qp.max_recv_wr = option.small_recv_wr();
  qp.max_inline_data = option.max_inline;
  return qp;
}

// Fresh psn per QP; machine-seeded so shards do not share a sequence.
static uint32_t NextPsn() {
  static thread_local std::mt19937 rng{std::random_device{}()};
  return rng() & 0xffffff;
}

static verbs::QpOption BulkLaneOption(const RdmaOption& option) {
  verbs::QpOption qp;
  qp.max_send_wr = option.bulk_send_wr;
  qp.max_recv_wr = 1;  // bulk lanes carry one-sided traffic only
  qp.max_send_sge = 1;
  // Small non-inlined one-sided writes cost the HCA an extra fetch.
  qp.max_inline_data = option.max_inline;
  return qp;
}

Status LaneSet::Init(RdmaDomain* domain) {
  const RdmaOption& option = domain->option();
  verbs::Device& device = domain->device();
  ibv_cq* cq = domain->cq().get();
  const uint8_t port_num = device.port().port_num;

  StatusOr<verbs::QueuePair> small =
      verbs::QueuePair::Create(device, cq, SmallLaneOption(option));
  if (!small.ok()) {
    return small.status();
  }
  small_qp_ = std::move(small).value();
  small_queue_ = std::make_unique<SendQueue>(
      small_qp_.get(), &domain->poller().dirty_sends(), option.small_send_wr(),
      small_qp_.max_inline());
  DINGOFS_RETURN_NOT_OK(small_qp_.ToInit(port_num));

  const verbs::QpOption bulk_option = BulkLaneOption(option);
  bulk_.reserve(option.bulk_lanes);
  for (uint8_t i = 0; i < option.bulk_lanes; ++i) {
    StatusOr<verbs::QueuePair> qp =
        verbs::QueuePair::Create(device, cq, bulk_option);
    if (!qp.ok()) {
      return qp.status();
    }
    BulkLane lane;
    lane.qp = std::move(qp).value();
    lane.queue = std::make_unique<SendQueue>(
        lane.qp.get(), &domain->poller().dirty_sends(), option.bulk_send_wr,
        lane.qp.max_inline());
    DINGOFS_RETURN_NOT_OK(lane.qp.ToInit(port_num));
    bulk_.push_back(std::move(lane));
  }

  local_psn_.resize(1 + bulk_.size());
  for (uint32_t& psn : local_psn_) {
    psn = NextPsn();
  }
  return Status::OK();
}

void LaneSet::Break() {
  small_qp_.ToError();
  small_queue_->SetBroken();
  for (BulkLane& lane : bulk_) {
    lane.qp.ToError();
    lane.queue->SetBroken();
  }
}

void LaneSet::FillPeer(const verbs::PortInfo& port, uint8_t rd_atomic,
                       HandshakeMsg* msg) {
  small_qp_.FillPeer(port, local_psn_[0], rd_atomic, &msg->qps[0]);
  for (size_t i = 0; i < bulk_.size(); ++i) {
    bulk_[i].qp.FillPeer(port, local_psn_[i + 1], rd_atomic, &msg->qps[i + 1]);
  }
}

Status LaneSet::ApplyPeer(const HandshakeMsg& msg, const verbs::PortInfo& port,
                          const verbs::QpOption& option, uint8_t rd_atomic) {
  DINGOFS_RETURN_NOT_OK(small_qp_.ToRtr(msg.qps[0], port, rd_atomic));
  DINGOFS_RETURN_NOT_OK(small_qp_.ToRts(local_psn_[0], rd_atomic, option));
  small_queue_->set_read_limit(rd_atomic);
  for (size_t i = 0; i < bulk_.size(); ++i) {
    DINGOFS_RETURN_NOT_OK(bulk_[i].qp.ToRtr(msg.qps[i + 1], port, rd_atomic));
    DINGOFS_RETURN_NOT_OK(
        bulk_[i].qp.ToRts(local_psn_[i + 1], rd_atomic, option));
    bulk_[i].queue->set_read_limit(rd_atomic);
  }
  DCHECK_EQ(small_qp_.QueryRdAtomic(), rd_atomic);
  return Status::OK();
}

SendQueue* LaneSet::NextBulk() {
  SendQueue* queue = bulk_[next_bulk_].queue.get();
  next_bulk_ = next_bulk_ + 1 == bulk_.size() ? 0 : next_bulk_ + 1;
  return queue;
}

uint32_t LaneSet::OutstandingWrs() const {
  uint32_t total = small_queue_ == nullptr ? 0 : small_queue_->inflight();
  for (const BulkLane& lane : bulk_) {
    total += lane.queue->inflight();
  }
  return total;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
