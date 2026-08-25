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

#include "blockcache/infiniband/client/context.h"

#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/core/memory/buffer.h"
#include "blockcache/core/memory/shard_allocator.h"
#include "blockcache/core/memory/slab_pool.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/poller.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

static size_t MessagePoolSuperblockCount() {
  const size_t bytes_per_connection =
      2 * size_t{Protocol::MessageBudget()} * size_t{FLAGS_rdma_message_bytes};
  const size_t total_bytes = bytes_per_connection * FLAGS_rdma_max_connections;
  return ((total_bytes + SlabPool::kSuperblockSize - 1) /
          SlabPool::kSuperblockSize) +
         1;
}

Status InfinibandContext::Create(std::string device_name) {
  CHECK(tls_infiniband_context == nullptr) << "one rdma context per shard";

  auto context = std::make_unique<InfinibandContext>();

  // device
  {
    StatusOr<Device*> device = Device::Open(std::move(device_name));
    if (!device.ok()) {
      return device.status();
    }
    context->device = device.value();
  }

  // memory registry
  {
    context->memory_registry =
        std::make_unique<MemoryRegistry>(context->device->pd());
    if (SlabPool* local = blockcache::BufferPool::LocalPool();
        local != nullptr) {
      StatusOr<const MemoryRegion*> mr = context->memory_registry->Register(
          local->base(), local->total_bytes());
      if (!mr.ok()) {
        return mr.status();
      }
    }
  }

  // buffer pool
  {
    SlabPoolOption option;
    option.superblock_count = MessagePoolSuperblockCount();
    option.numa_node = memory::LocalNumaNode();
    context->buffer_pool = std::make_unique<BufferPool>(option);
    Status status = context->buffer_pool->Init(context->memory_registry.get());
    if (!status.ok()) {
      return status;
    }
  }

  // completion channel
  {
    StatusOr<CompletionChannel> completion_channel =
        CompletionChannel::Create(*context->device);
    if (!completion_channel.ok()) {
      return completion_channel.status();
    }
    context->completion_channel = std::make_unique<CompletionChannel>(
        std::move(completion_channel).value());
  }

  // completion queue
  {
    StatusOr<CompletionQueue> completion_queue =
        CompletionQueue::Create(*context->device, *context->completion_channel);
    if (!completion_queue.ok()) {
      return completion_queue.status();
    }
    context->completion_queue =
        std::make_unique<CompletionQueue>(std::move(completion_queue).value());
  }

  // poller
  {
    context->poller = std::make_unique<InfinibandPoller>(
        context->completion_queue.get(), context->completion_channel->fd());
  }

  tls_infiniband_context = std::move(context);
  return Status::OK();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
