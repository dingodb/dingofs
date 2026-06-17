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

/*
 * Project: DingoFS
 * Created Date: 2026-06-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/reader.h"

#include "cache/infiniband/common.h"

namespace dingofs {
namespace cache {
namespace infiniband {

BodyReader::BodyReader(Connection* conn) : conn_(conn) {}

Status BodyReader::Read(RDMABuffer* dst, const std::vector<Region>& src,
                        size_t size) {
  auto status = CheckSource(src, size);
  if (!status.ok()) {
    return status;
  }

  InflightContext ctx(src.size());
  std::vector<SendWorkRequest> work_requests(src.size());
  PrepWorkRequests(dst, src, &ctx, &work_requests);
  status = conn_->PostSendWorkRequests(work_requests);
  if (!status.ok()) {
    return status;
  }

  ctx.inflights.wait();
  return ctx.status;
}

Status BodyReader::CheckSource(const std::vector<Region>& regions,
                               size_t size) {
  size_t total_length = 0;
  for (const auto& region : regions) {
    if (region.addr == 0 || region.rkey == 0) {
      return Status::Internal("request rdma memory context is missing");
    }
    total_length += region.length;
  }

  if (regions.empty() || total_length != size) {
    return Status::Internal(
        "request attachment size mismatches advertised rdma regions");
  }
  return Status::OK();
}

void BodyReader::PrepWorkRequests(RDMABuffer* dst,
                                  const std::vector<Region>& regions,
                                  InflightContext* ctx,
                                  std::vector<SendWorkRequest>* work_requests) {
  uint32_t offset = 0;
  SendWorkRequest wr;
  for (int i = 0; i < regions.size(); i++) {
    wr.opcode = OpCode::kRDMARead;
    wr.addr = reinterpret_cast<uint64_t>(dst->data + offset);
    wr.length = regions[i].length;
    wr.lkey = dst->lkey;
    wr.raddr = regions[i].addr;
    wr.rkey = regions[i].rkey;
    wr.signaled = true;
    wr.ctx = &ctx->contexts[i];
    wr.ctx->on_completion = [ctx](const WorkCompletion& wc) {
      if (!wc.status.ok()) {
        ctx->status = wc.status;
      }
      ctx->inflights.signal(1);
    };

    work_requests->push_back(wr);
    offset += regions[i].length;
  }
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
