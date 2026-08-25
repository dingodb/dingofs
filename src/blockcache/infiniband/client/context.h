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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CONTEXT_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CONTEXT_H_

#include <memory>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/infiniband/base/buffer_pool.h"
#include "blockcache/infiniband/base/completion_channel.h"
#include "blockcache/infiniband/base/completion_queue.h"
#include "blockcache/infiniband/base/device.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/connection/poller.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

struct InfinibandContext;
using InfinibandContextUPtr = std::unique_ptr<InfinibandContext>;

struct InfinibandContext {
  static Status Create(std::string device_name);

  Device* device = nullptr;
  BufferPoolUPtr buffer_pool;
  MemoryRegistryUPtr memory_registry;
  CompletionChannelUPtr completion_channel;
  CompletionQueueUPtr completion_queue;
  InfinibandPollerUPtr poller;
};

inline thread_local InfinibandContextUPtr tls_infiniband_context;

inline InfinibandContext* ThisInfinibandContext() {
  return tls_infiniband_context.get();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
