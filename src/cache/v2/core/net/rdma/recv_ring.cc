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

#include "cache/v2/core/net/rdma/recv_ring.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstring>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/wr_id.h"

namespace dingofs {
namespace cache {
namespace v2 {

RecvRing::~RecvRing() {
  for (auto& slot : slots_) {
    if (slot.data != nullptr) {
      pool_->Free(slot.data);
    }
  }
}

Status RecvRing::Init(ibv_qp* qp, RdmaConnection* conn, SlabPool* pool,
                      uint32_t lkey, uint16_t count, uint32_t buffer_bytes) {
  qp_ = qp;
  pool_ = pool;
  lkey_ = lkey;
  buffer_bytes_ = buffer_bytes;

  slots_.resize(count);
  wrs_.resize(count);
  sges_.resize(count);
  pending_.reserve(count);

  for (uint16_t i = 0; i < count; ++i) {
    char* data = pool->Alloc(buffer_bytes);
    if (data == nullptr) {
      return ToStatus(ENOMEM,
                         "carve a receive buffer from the slab pool: the pool "
                         "holds fewer connections than this shard opened, "
                         "raise RdmaOption::max_connections");
    }
    slots_[i].conn = conn;
    slots_[i].data = data;
    slots_[i].idx = i;
  }
  return Status::OK();
}

void RecvRing::PostAll() {
  const auto n = static_cast<uint16_t>(slots_.size());
  std::vector<uint16_t> all(n);
  for (uint16_t i = 0; i < n; ++i) {
    all[i] = i;
  }
  PostSlots(all.data(), static_cast<uint32_t>(all.size()));
}

void RecvRing::Recycle(uint16_t recv_slot) {
  if (dead_) {
    return;
  }
  pending_.push_back(recv_slot);
  if (pending_.size() >= kReplenishBatch) {
    FlushReplenish();
  }
}

void RecvRing::FlushReplenish() {
  if (pending_.empty() || dead_) {
    pending_.clear();
    return;
  }
  PostSlots(pending_.data(), static_cast<uint32_t>(pending_.size()));
  pending_.clear();
}

// One chained ibv_post_recv for the whole batch: one doorbell, not n.
void RecvRing::PostSlots(const uint16_t* indices, uint32_t n) {
  if (n == 0 || dead_) {
    return;
  }
  for (uint32_t i = 0; i < n; ++i) {
    const uint16_t idx = indices[i];
    ibv_sge* sge = &sges_[idx];
    sge->addr = reinterpret_cast<uint64_t>(slots_[idx].data);
    sge->length = buffer_bytes_;
    sge->lkey = lkey_;

    ibv_recv_wr* wr = &wrs_[idx];
    wr->wr_id = MakeWrId(&slots_[idx], kTagRecv);
    wr->sg_list = sge;
    wr->num_sge = 1;
    wr->next = i + 1 < n ? &wrs_[indices[i + 1]] : nullptr;
  }

  ibv_recv_wr* bad = nullptr;
  int rc = ibv_post_recv(qp_, &wrs_[indices[0]], &bad);
  if (rc != 0) {
    LOG(ERROR) << "Fail to post recv work request: " << std::strerror(rc);
    dead_ = true;
    return;
  }
  outstanding_ += n;
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
