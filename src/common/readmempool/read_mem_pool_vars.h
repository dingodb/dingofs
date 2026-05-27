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

#ifndef DINGOFS_SRC_COMMON_READMEMPOOL_READ_MEM_POOL_VARS_H_
#define DINGOFS_SRC_COMMON_READMEMPOOL_READ_MEM_POOL_VARS_H_

#include <bvar/passive_status.h>
#include <bvar/status.h>

#include <cstdint>
#include <string>

#include "common/readmempool/read_mem_pool.h"

namespace dingofs {

// Every var is named "{prefix}_read_mempool_{metric}" (built once, here).
#define DINGOFS_RMP_NAME(metric) (prefix + "_read_mempool_" metric)

// A PassiveStatus<int64_t> whose scrape thunk reads one pool getter. A
// captureless lambda decays to the int64_t(*)(void*) the ctor wants (+[] pins
// that decay); the pool ptr is the void* arg.
#define DINGOFS_RMP_VAR(field, metric, getter)                           \
  field(DINGOFS_RMP_NAME(metric), +[](void* p) -> int64_t {              \
    return static_cast<int64_t>(static_cast<ReadMemPool*>(p)->getter()); \
  }, pool)

// Publishes a (singleton) ReadMemPool's metrics under brpc /vars as
// {prefix}_read_mempool_* (the caller supplies the subsystem prefix, e.g.
// "vfs", so this common component doesn't bake in a client naming convention).
// The pool itself keeps anonymous bvar::Adder counters (so multiple pool
// instances don't collide on global bvar names); this layer wraps named
// PassiveStatus that read the pool's getters at scrape time. Construct one per
// pool; its lifetime must be a subset of the pool's.
//
// usage_ratio is intentionally NOT exposed: derive it monitoring-side from
// outstanding_bytes / total_bytes.
class ReadMemPoolVars {
 public:
  ReadMemPoolVars(ReadMemPool* pool, const std::string& prefix)
      : total_bytes_(DINGOFS_RMP_NAME("total_bytes"),
                     static_cast<int64_t>(pool->TotalSize())),
        DINGOFS_RMP_VAR(outstanding_bytes_, "outstanding_bytes",
                        OutstandingBytes),
        DINGOFS_RMP_VAR(buddy_used_bytes_, "buddy_used_bytes", BuddyUsedBytes),
        DINGOFS_RMP_VAR(slab_occupied_bytes_, "slab_occupied_bytes",
                        SlabOccupiedBytes),
        DINGOFS_RMP_VAR(slab_used_bytes_, "slab_used_bytes", SlabUsedBytes),
        DINGOFS_RMP_VAR(tls_cached_bytes_, "tls_cached_bytes", TlsCachedBytes),
        DINGOFS_RMP_VAR(largest_free_order_, "largest_free_order",
                        LargestFreeOrder),
        DINGOFS_RMP_VAR(alloc_num_, "alloc_num", AllocNum),
        DINGOFS_RMP_VAR(alloc_fail_num_, "alloc_fail_num", AllocFailNum),
        DINGOFS_RMP_VAR(drain_reclaimed_bytes_, "drain_reclaimed_bytes",
                        DrainReclaimedBytes),
        DINGOFS_RMP_VAR(tls_hit_, "tls_hit", TlsHitCount),
        DINGOFS_RMP_VAR(tls_miss_, "tls_miss", TlsMissCount) {}

 private:
  bvar::Status<int64_t> total_bytes_;
  bvar::PassiveStatus<int64_t> outstanding_bytes_;
  bvar::PassiveStatus<int64_t> buddy_used_bytes_;
  bvar::PassiveStatus<int64_t> slab_occupied_bytes_;
  bvar::PassiveStatus<int64_t> slab_used_bytes_;
  bvar::PassiveStatus<int64_t> tls_cached_bytes_;
  bvar::PassiveStatus<int64_t> largest_free_order_;
  bvar::PassiveStatus<int64_t> alloc_num_;
  bvar::PassiveStatus<int64_t> alloc_fail_num_;
  bvar::PassiveStatus<int64_t> drain_reclaimed_bytes_;
  bvar::PassiveStatus<int64_t> tls_hit_;
  bvar::PassiveStatus<int64_t> tls_miss_;
};

#undef DINGOFS_RMP_VAR
#undef DINGOFS_RMP_NAME

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_READMEMPOOL_READ_MEM_POOL_VARS_H_
