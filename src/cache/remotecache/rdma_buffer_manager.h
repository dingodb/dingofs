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
 * Created Date: 2026-05-31
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_BUFFER_MANAGER_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_BUFFER_MANAGER_H_

#include <mutex>

#include "cache/infiniband/memory.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

// Owns the client-side registered RDMA buffer pool and turns plain IOBuffers
// into RDMA-advertisable ones (single registered block whose first-block meta
// carries the rkey). RemoteBlockCache uses it to prepare Put/Cache sources and
// Range destinations at the cache layer; the transport then merely reads
// addr/len/rkey from the IOBuffer and posts the work request — no allocation or
// copy in the transport. Process-wide singleton (one client RDMA context per
// process); the pool is created lazily on first use.
class RDMABufferManager {
 public:
  static RDMABufferManager& GetInstance();

  // True when --use_rdma is set and the registered pool is available.
  bool Enabled();

  // A fresh registered buffer of `size` bytes (meta=rkey; the deleter returns
  // it to the pool when the IOBuffer is destroyed). Used as a Range destination
  // the server RDMA-writes into. Empty IOBuffer on failure.
  IOBuffer NewBuffer(size_t size);

  // `src` itself when it is already a single registered block (zero copy),
  // otherwise a registered copy. Used to prepare Put/Cache upload sources.
  IOBuffer EnsureRegistered(const IOBuffer& src);

 private:
  RDMABufferManager() = default;

  infiniband::RDMABufferPool* Pool();  // lazily initialized

  std::once_flag once_;
  infiniband::RDMABufferPoolUPtr pool_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_RDMA_BUFFER_MANAGER_H_
