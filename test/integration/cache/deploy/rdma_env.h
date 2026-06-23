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
 * Created Date: 2026-06-22
 * Author: AI
 */

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_RDMA_ENV_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_RDMA_ENV_H_

#include <gflags/gflags.h>

#include "cache/common/slab_buffer.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {
namespace integration {

// Whether the process-wide cache slab pool was successfully initialized.
//
// The on-disk cache (in this test process AND in the spawned dingo-cache)
// stages its O_DIRECT/io_uring buffers through the global slab pool. The pool
// no longer requires RDMA: the whole suite drives everything over TCP (use_rdma
// off), so the pool is plain pinned memory and the tests run on any host. It is
// only unavailable when the pool itself cannot be built (e.g. the environment
// cannot back the pinned mapping).
inline bool& SlabPoolReady() {
  static bool ready = false;
  return ready;
}

// Initialize the global slab pool for the suite. No RDMA device is needed: with
// use_rdma off InitializeGlobalSlabPool() builds a plain pool. Call once after
// ParseCommandLineFlags.
inline bool InitSlabPoolForTests() {
  // The slab pool pins kSlabBufferSize(4MiB) * iodepth * 2 of memory. Keep the
  // test footprint modest (default iodepth is 128 -> ~1GiB).
  if (FLAGS_iodepth > 16) {
    FLAGS_iodepth = 16;
  }

  SlabPoolReady() = InitializeGlobalSlabPool().ok();
  return SlabPoolReady();
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_RDMA_ENV_H_
