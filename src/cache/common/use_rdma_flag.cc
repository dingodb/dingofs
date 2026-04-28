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
 * Created Date: 2026-05-20
 * Author: Jingli Chen (Wine93)
 */

#include <gflags/gflags.h>

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

DEFINE_bool(use_rdma, false,
            "Enable Infiniband/RDMA transport for cache RPCs. When true, the "
            "cache server starts an RDMA service alongside brpc, and the "
            "client routes Range/Cache/Put RPCs through the RDMA path. Server "
            "and client must agree on the value for the RDMA path to be used.");

// Prefixed with cache_ to avoid colliding with brpc's own --rdma_device /
// --rdma_port flags (brpc/rdma/rdma_helper.cpp).
DEFINE_string(cache_rdma_device, "mlx5_0",
              "IB device used by the cache RDMA path");
DEFINE_uint32(cache_rdma_port_num, 1,
              "HCA port (1-based) used by the cache RDMA path");

}  // namespace cache
}  // namespace dingofs
