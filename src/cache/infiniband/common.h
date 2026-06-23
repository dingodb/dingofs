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
 * Created Date: 2026-06-15
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_COMMON_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_COMMON_H_

#include <bthread/countdown_event.h>

#include <cstdint>
#include <vector>

#include "cache/common/slab_buffer.h"
#include "cache/infiniband/controller.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

struct WorkCompletion;
using OnCompletion = std::function<void(const WorkCompletion& wc)>;

struct EndPoint {
  std::string device_name;  // IB device, e.g. "mlx5_0"
  uint8_t port_num{0};      // HCA port, 1-based
};

enum class OpCode : uint8_t {
  kUnknown = 0,
  kSend = 1,
  kRecv = 2,
  kRDMAWrite = 3,
  kRDMARead = 4,
};

struct WorkRequestContext {
  OnCompletion on_completion;
};

struct SendWorkRequest {
  OpCode opcode{OpCode::kUnknown};

  // local
  uint64_t addr{0};
  uint32_t length{0};
  uint32_t lkey{0};

  // remote
  uint64_t raddr{0};
  uint32_t rkey{0};

  bool signaled{false};
  WorkRequestContext* ctx{nullptr};
};

struct RecvWorkRequest {
  // local
  uint64_t addr{0};
  uint32_t length{0};
  uint32_t lkey{0};

  WorkRequestContext* ctx{nullptr};
};

struct WorkCompletion {
  WorkRequestContext* ctx{nullptr};
  OpCode opcode{OpCode::kUnknown};
  Status status{Status::Unknown("unknown")};
  uint32_t byte_len{0};
};

using WorkCompletions = std::vector<WorkCompletion>;

using RDMABuffer = ::dingofs::cache::SlabBuffer;

struct Region {
  Region() = default;

  Region(const pb::infiniband::RDMARegion& region)
      : addr(region.addr()), length(region.length()), rkey(region.rkey()) {}

  uint64_t addr{0};
  uint32_t length{0};
  uint32_t rkey{0};
};

struct Attachment {
  IOBuffer buffer;
  Region dest;
};

struct InflightContext {
  explicit InflightContext(int n) : status(Status::OK()) {
    inflights.reset(n);
    contexts.resize(n);
  }

  Status status;
  bthread::CountdownEvent inflights;
  std::vector<WorkRequestContext> contexts;
};

inline std::vector<Region> FromPbRegions(
    const google::protobuf::RepeatedPtrField<pb::infiniband::RDMARegion>&
        pb_regions) {
  return std::vector<Region>(pb_regions.begin(), pb_regions.end());
}

inline void ToPbRegion(const Region& region,
                       pb::infiniband::RDMARegion* pb_region) {
  pb_region->set_addr(region.addr);
  pb_region->set_length(region.length);
  pb_region->set_rkey(region.rkey);
}

inline void ToPbRegions(
    const std::vector<Region>& regions,
    google::protobuf::RepeatedPtrField<pb::infiniband::RDMARegion>*
        pb_regions) {
  pb_regions->Clear();
  pb_regions->Reserve(static_cast<int>(regions.size()));
  for (const auto& region : regions) {
    ToPbRegion(region, pb_regions->Add());
  }
}

inline void SetFailed(Controller* cntl, pb::infiniband::ErrorCode error_code,
                      const std::string& reason) {
  cntl->SetErrorCode(error_code);
  cntl->SetFailed(reason);
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_COMMON_H_
