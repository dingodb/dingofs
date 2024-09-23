/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_METRIC_H_
#define CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_METRIC_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "curvefs/src/client/blockcache/block_cache_uploader.h"
#include "curvefs/src/client/common/config.h"
#include "src/common/concurrent/task_thread_pool.h"

namespace curvefs {
namespace client {
namespace blockcache {

using ::curve::common::TaskThreadPool;
using ::curvefs::client::common::BlockCacheOption;

static uint32_t GetQueueSize(void* arg) {
  auto* thread_pool = reinterpret_cast<TaskThreadPool<>*>(arg);
  return thread_pool->QueueSize();
}

static size_t GetPendingBlocks(void* arg) {
  auto* uploader = reinterpret_cast<BlockCacheUploader*>(arg);
  return uploader->GetPendingSize();
}

class BlockCacheMetric {
 public:
  struct AuxMember {
    std::shared_ptr<BlockCacheUploader> uploader;
  };

 public:
  BlockCacheMetric(BlockCacheOption option, AuxMember aux_member)
      : metric_("dingofs_block_cache", aux_member) {
    metric_.upload_stage_workers.set_value(option.upload_stage_workers);
    metric_.upload_stage_queue_size.set_value(option.upload_stage_queue_size);
  }

  virtual ~BlockCacheMetric() = default;

 private:
  struct Metric {
    Metric(const std::string& prefix, AuxMember aux_member)
        : upload_stage_workers(prefix, "upload_stage_workers", 0),
          upload_stage_queue_size(prefix, "upload_stage_queue_size", 0),
          upload_stage_pending_blocks(prefix, "upload_stage_pending_blocks",
                                      &GetPendingBlocks,
                                      aux_member.uploader.get()),
          upload_stage_pending_tasks(
              prefix, "upload_stage_pending_tasks", &GetQueueSize,
              aux_member.uploader->upload_stage_thread_pool_.get()) {}

    bvar::Status<uint32_t> upload_stage_workers;
    bvar::Status<uint32_t> upload_stage_queue_size;
    bvar::PassiveStatus<size_t> upload_stage_pending_blocks;
    bvar::PassiveStatus<uint32_t> upload_stage_pending_tasks;
  };

  Metric metric_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BLOCKCACHE_BLOCK_CACHE_METRIC_H_
