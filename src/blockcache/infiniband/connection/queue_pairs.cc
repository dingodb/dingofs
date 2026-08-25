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

#include "blockcache/infiniband/connection/queue_pairs.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cstring>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/queue_pair.h"
#include "blockcache/infiniband/common/protocol.h"

namespace dingofs {
namespace blockcache {

DEFINE_uint32(rdma_max_inline_data, 192, "max bytes inlined into a wqe");

DEFINE_uint32(rdma_bulk_send_wr, 256,
              "send queue depth of one bulk queue pair");
DEFINE_validator(rdma_bulk_send_wr, [](const char* /*name*/, uint32_t value) {
  return value > 0;
});

DEFINE_uint32(rdma_bulk_qps, 2, "bulk queue pairs per connection");
DEFINE_validator(rdma_bulk_qps, [](const char* /*name*/, uint32_t value) {
  return value > 0 && value <= infiniband::kMaxBulkQps;
});

namespace infiniband {

StatusOr<QueuePairGroup> QueuePairGroup::Create(
    Device* device, CompletionQueue* completion_queue) {
  QueuePairGroup group;

  // msg queue pair
  {
    QueuePairOption option;
    option.max_send_wr = Protocol::MsgSendWr();
    option.max_recv_wr = Protocol::MsgRecvWr();
    option.max_inline_data = FLAGS_rdma_max_inline_data;

    StatusOr<QpRail> msg = CreateQpRail(device, completion_queue, option);
    if (!msg.ok()) {
      return msg.status();
    }
    group.msg_ = std::move(msg).value();
  }

  // bulk queue pair
  {
    QueuePairOption option;
    option.max_send_wr = FLAGS_rdma_bulk_send_wr;
    option.max_recv_wr = 1;
    option.max_send_sge = 1;
    option.max_inline_data = FLAGS_rdma_max_inline_data;

    const auto count = static_cast<uint8_t>(FLAGS_rdma_bulk_qps);
    group.bulks_.reserve(count);
    for (uint8_t i = 0; i < count; ++i) {
      StatusOr<QpRail> bulk = CreateQpRail(device, completion_queue, option);
      if (!bulk.ok()) {
        return bulk.status();
      }
      group.bulks_.push_back(std::move(bulk).value());
    }
  }

  return group;
}

StatusOr<QueuePairGroup::QpRail> QueuePairGroup::CreateQpRail(
    Device* device, CompletionQueue* completion_queue,
    const QueuePairOption& option) {
  StatusOr<QueuePair> queue_pair =
      QueuePair::Create(*device, completion_queue->get(), option);
  if (!queue_pair.ok()) {
    return queue_pair.status();
  }

  QpRail rail;
  rail.queue_pair = std::move(queue_pair).value();
  rail.queue = SendQueue(&rail.queue_pair, option.max_send_wr);
  Status status = rail.queue_pair.ModifyToInit();
  if (!status.ok()) {
    return status;
  }
  return rail;
}

Status QueuePairGroup::ModifyToReady(std::span<const QueuePairInfo> peers) {
  CHECK_EQ(peers.size(), bulks_.size() + 1) << "one peer info per queue pair";

  Status status = ModifyToReady(&msg_, peers[0]);
  if (!status.ok()) {
    return status;
  }

  for (size_t i = 0; i < bulks_.size(); ++i) {
    status = ModifyToReady(&bulks_[i], peers[i + 1]);
    if (!status.ok()) {
      return status;
    }
  }

  return Status::OK();
}

Status QueuePairGroup::ModifyToReady(QpRail* rail, const QueuePairInfo& peer) {
  Status status = rail->queue_pair.ModifyToInit();
  if (!status.ok()) {
    return status;
  }

  status = rail->queue_pair.ModifyToRtr(peer);
  if (!status.ok()) {
    return status;
  }

  status = rail->queue_pair.ModifyToRts();
  if (!status.ok()) {
    return status;
  }

  rail->queue.Start();
  return Status::OK();
}

void QueuePairGroup::ModifyToError() {
  msg_.queue_pair.ModifyToError();
  msg_.queue.Shutdown();

  for (QpRail& bulk : bulks_) {
    bulk.queue_pair.ModifyToError();
    bulk.queue.Shutdown();
  }
}

SendQueue* QueuePairGroup::NextBulkQueue() {
  SendQueue* queue = &bulks_[next_bulk_].queue;
  next_bulk_ = next_bulk_ + 1 == bulks_.size() ? 0 : next_bulk_ + 1;
  return queue;
}

uint32_t QueuePairGroup::bulk_inflights() const {
  uint32_t total = 0;
  for (const QpRail& bulk : bulks_) {
    total += bulk.queue.inflights();
  }
  return total;
}

std::vector<QueuePairInfo> QueuePairGroup::GetInfos() const {
  std::vector<QueuePairInfo> infos;
  infos.reserve(qp_count());
  infos.push_back(msg_.queue_pair.GetInfo());
  for (const QpRail& bulk : bulks_) {
    infos.push_back(bulk.queue_pair.GetInfo());
  }
  return infos;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
