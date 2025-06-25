/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include "client/vfs/data/slice/task/slice_flush_task.h"

#include <glog/logging.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

void SliceFlushTask::FlushDone(Status s) {
  VLOG(4) << fmt::format("{} FlushDone status: {}", UUID(), s.ToString());

  StatusCallback cb;
  {
    std::lock_guard<std::mutex> lg(mutex_);
    cb.swap(cb_);
  }

  cb(s);

  VLOG(4) << fmt::format("{} End slice flush status: {}", UUID(), s.ToString());
}

// callback from block cache, maybe in bthread
// Add callback pool to exec thi callback
void SliceFlushTask::BlockDataFlushed(BlockData* block_data, Status status) {
  VLOG(6) << fmt::format("{} BlockDataFlushed block_data: {}, status: {} ",
                         UUID(), block_data->UUID(), status.ToString());

  if (!status.ok()) {
    LOG(WARNING) << fmt::format("{} Failed to flush block_data: {}, status: {}",
                                UUID(), block_data->UUID(), status.ToString());

    std::lock_guard<std::mutex> lg(mutex_);
    // TODO: save all errors
    status_ = status;
  }

  if (flush_block_data_count_.fetch_sub(1) == 1) {
    Status flush_status;
    {
      std::lock_guard<std::mutex> lg(mutex_);
      flush_status = status_;
    }
    FlushDone(flush_status);
  }
}

void SliceFlushTask::RunAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("{} Start slice flush", UUID());

  if (block_datas_.empty()) {
    VLOG(1) << fmt::format("{} End slice flush because no block_data to flush",
                           UUID());
    cb(Status::OK());
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    cb_ = std::move(cb);
    status_ = Status::OK();
  }

  flush_block_data_count_.store(block_datas_.size(), std::memory_order_relaxed);

  for (const auto& [block_index, block_data_ptr] : block_datas_) {
    BlockData* block_data = block_data_ptr.get();
    VLOG(6) << fmt::format("{} flush block_data: {}", UUID(),
                           block_data->ToString());
    DCHECK_EQ(block_data->BlockIndex(), block_index);

    IOBuffer io_buffer = block_data->ToIOBuffer();

    // TODO: read write back option from somewhere, currently using default
    cache::PutOption option;
    // TODO: Block should  take own the iobuf
    cache::BlockKey key(slice_data_context_.fs_id, slice_data_context_.ino,
                        slice_id_, block_index, 0);
    vfs_hub_->GetBlockCache()->AsyncPut(
        key, cache::Block(io_buffer),
        [this, block_data](auto&& ph1) {
          BlockDataFlushed(block_data, std::forward<decltype(ph1)>(ph1));
        },
        option);

    VLOG(6) << fmt::format(
        "{} Scheduled flush for block_data: {}, cache_block_key: {}", UUID(),
        block_data->UUID(), key.StoreKey());
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
