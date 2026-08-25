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

#include "blockcache/infiniband/connection/sender.h"

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <ios>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/common/wr_id.h"
#include "blockcache/infiniband/connection/send_queue.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

static SendWorkRequest BuildWorkRequest(ibv_wr_opcode opcode,
                                        const LocalRegion& local,
                                        uint64_t remote_addr, uint32_t rkey,
                                        uint32_t max_inline_data) {
  SendWorkRequest work_request;
  work_request.sge.addr = reinterpret_cast<uint64_t>(local.addr);
  work_request.sge.length = local.len;
  work_request.sge.lkey = local.lkey;

  work_request.wr.num_sge = local.len == 0 ? 0 : 1;
  work_request.wr.opcode = opcode;
  if ((opcode == IBV_WR_SEND || opcode == IBV_WR_RDMA_WRITE) &&
      local.len <= max_inline_data) {
    work_request.wr.send_flags = IBV_SEND_INLINE;
  }

  if (opcode == IBV_WR_RDMA_WRITE || opcode == IBV_WR_RDMA_READ) {
    work_request.wr.wr.rdma.remote_addr = remote_addr;
    work_request.wr.wr.rdma.rkey = rkey;
  }
  return work_request;
}

OpBatch::OpBatch(SendQueue* queue, uint32_t capacity)
    : queue_(queue), work_requests_(capacity) {}

void OpBatch::Add(SendWorkRequest work_request) {
  DCHECK_LT(num_wrs_, work_requests_.size());
  work_requests_[num_wrs_] = work_request;
  if (num_wrs_ != 0) {
    work_requests_[num_wrs_ - 1].wr.next = &work_requests_[num_wrs_].wr;
  }

  ++num_wrs_;
  if (work_request.wr.opcode == IBV_WR_RDMA_READ) {
    ++num_read_wrs_;
  }
}

BatchAwaiter OpBatch::Submit() {
  ibv_send_wr* first_wr = num_wrs_ != 0 ? &work_requests_.front().wr : nullptr;
  ibv_send_wr* last_wr =
      num_wrs_ != 0 ? &work_requests_[num_wrs_ - 1].wr : nullptr;
  return BatchAwaiter(queue_, first_wr, last_wr, num_wrs_, num_read_wrs_);
}

Status MsgSender::Send(SendBuffer* buffer, size_t len) {
  const LocalRegion local{.addr = buffer->data,
                          .len = static_cast<uint32_t>(len),
                          .lkey = buffers_->lkey()};

  SendWorkRequest request =
      BuildWorkRequest(IBV_WR_SEND, local, /*remote_addr=*/0, /*rkey=*/0,
                       send_queue_->max_inline_data());
  ibv_send_wr* wr = &request.wr;
  wr->wr_id = MakeWrId(buffer, kTagSendBuffer);
  wr->send_flags |= IBV_SEND_SIGNALED;
  if (!send_queue_->TrySubmit(wr)) {
    return ToStatus(ENOBUFS, "post a message");
  }
  return Status::OK();
}

void MsgSender::Countdown() { send_queue_->Release(1, 0); }

BulkSender::BulkSender(QueuePairGroup* qps)
    : qps_(qps), registry_(CHECK_NOTNULL(ThisMemoryRegistry())) {}

Future<Status> BulkSender::Move(BufferView buffer,
                                std::span<const RemoteRegion> regions,
                                bool is_read) {
  Status status = Check(buffer, regions);
  if (!status.ok()) {
    co_return status;
  }

  StatusOr<uint32_t> lkey = registry_->GetLKey(buffer.data, buffer.size);
  if (!lkey.ok()) {
    co_return ToStatus(EINVAL, "move an attachment: unregistered buffer");
  }

  Walker walker{.opcode = is_read ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE,
                .lkey = lkey.value(),
                .regions = regions,
                .local_addr = static_cast<char*>(buffer.data),
                .remaining = buffer.size};

  while (!walker.done() && status.ok()) {
    SendQueue* queue = qps_->NextBulkQueue();
    const uint32_t available_wrs = queue->AvailableWrs(is_read);
    if (available_wrs == 0) {
      status = co_await OpAwaiter(
          queue, walker.NextWorkRequest(queue->max_inline_data()));
    } else {
      OpBatch batch = BuildBatch(queue, available_wrs, &walker);
      status = co_await batch.Submit();
    }
  }

  if (!status.ok()) {
    LOG_EVERY_N(ERROR, 100)
        << "Fail to " << (is_read ? "read" : "write")
        << " an attachment: local=" << buffer.data << "+" << buffer.size
        << " lkey=" << lkey.value() << " remote=0x" << std::hex
        << regions[0].addr << std::dec << "+" << regions[0].len
        << " rkey=" << regions[0].rkey << " regions=" << regions.size() << ": "
        << status.ToString();
  }
  co_return status;
}

SendWorkRequest BulkSender::Walker::NextWorkRequest(uint32_t max_inline_data) {
  DCHECK(!done());

  const RemoteRegion& remote = regions[region_index++];
  const uint32_t len =
      static_cast<uint32_t>(std::min<uint64_t>(remote.len, remaining));
  const LocalRegion local{.addr = local_addr, .len = len, .lkey = lkey};
  local_addr += len;
  remaining -= len;

  return BuildWorkRequest(opcode, local, remote.addr, remote.rkey,
                          max_inline_data);
}

OpBatch BulkSender::BuildBatch(SendQueue* queue, uint32_t max_wrs,
                               Walker* walker) {
  const size_t batch_size =
      std::min<size_t>(max_wrs, walker->remaining_regions());
  OpBatch batch(queue, static_cast<uint32_t>(batch_size));
  for (size_t i = 0; i < batch_size && !walker->done(); ++i) {
    batch.Add(walker->NextWorkRequest(queue->max_inline_data()));
  }
  return batch;
}

Status BulkSender::Check(BufferView buffer,
                         std::span<const RemoteRegion> regions) const {
  if (buffer.empty()) {
    return ToStatus(EINVAL, "move an attachment: empty buffer");
  }
  if (regions.empty()) {
    return ToStatus(EINVAL, "move an attachment: the peer sent no region");
  }
  if (buffer.size > GetLength(regions)) {
    return ToStatus(EMSGSIZE,
                    "move an attachment: it exceeds the peer's regions");
  }
  return Status::OK();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
