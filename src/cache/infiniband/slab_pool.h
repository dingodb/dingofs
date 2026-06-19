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
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_SLAB_POOL_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_SLAB_POOL_H_

#include <bits/types/struct_iovec.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "cache/infiniband/common.h"
#include "cache/infiniband/memory.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {
namespace infiniband {

Status InitializeGlobalSlabPool();
RDMABufferPool* GetGlobalWriteSlabPool();
RDMABufferPool* GetGlobalReadSlabPool();

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SLAB_POOL_H_
