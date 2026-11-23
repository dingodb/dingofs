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

#include "blockcache/core/reactor/uring.h"

#include <glog/logging.h>
#include <liburing.h>
#include <liburing/io_uring.h>

#include <cstdint>
#include <span>
#include <string>

namespace dingofs {
namespace blockcache {

void CheckUringSupport(io_uring* ring, const io_uring_params& params,
                       std::span<const UringOpcode> ops) {
  constexpr uint32_t kNeeded = IORING_FEAT_SUBMIT_STABLE | IORING_FEAT_NODROP;
  if ((params.features & kNeeded) != kNeeded) {
    io_uring_queue_exit(ring);
    LOG(FATAL) << "Fail to use io_uring: kernel lacks SUBMIT_STABLE|NODROP";
  }

  io_uring_probe* probe = io_uring_get_probe_ring(ring);
  if (probe == nullptr) {
    return;
  }

  std::string missing;
  for (const UringOpcode& op : ops) {
    if (!io_uring_opcode_supported(probe, op.op)) {
      missing += op.name;
      missing += ' ';
    }
  }

  io_uring_free_probe(probe);
  if (!missing.empty()) {
    LOG(FATAL) << "Fail to use io_uring: missing opcodes: " << missing;
  }
}

bool IsUringOpSupported(int op) {
  static io_uring_probe* const probe = io_uring_get_probe();
  return probe != nullptr && io_uring_opcode_supported(probe, op);
}

}  // namespace blockcache
}  // namespace dingofs
