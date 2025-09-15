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

#ifndef DINGODB_CLIENT_VFS_DATA_SLICE_FLUSH_TASK_H_
#define DINGODB_CLIENT_VFS_DATA_SLICE_FLUSH_TASK_H_

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "client/vfs/data/slice/block_data.h"
#include "client/vfs/data/slice/common.h"
#include "common/callback.h"

namespace dingofs {
namespace client {
namespace vfs {

class VFSHub;
class SliceFlushTask {
 public:
  SliceFlushTask(const SliceDataContext& context, VFSHub* hub,
                 uint64_t slice_id,
                 std::map<uint64_t, BlockDataUPtr> block_datas)
      : slice_data_context_(context),
        vfs_hub_(hub),
        slice_id_(slice_id),
        block_count_(block_datas.size()),
        block_datas_(std::move(block_datas)) {}

  ~SliceFlushTask() = default;

  void RunAsync(StatusCallback cb);

  std::string UUID() const {
    return fmt::format("slice_flush_task-{}-{}", slice_data_context_.UUID(),
                       slice_id_);
  }

  std::string ToString() const {
    return fmt::format("(uuid: {}, blocks_count: {})", UUID(), block_count_);
  }

 private:
  void BlockDataFlushedFromBlockCache(BlockData* block_data, Status status);
  void BlockDataFlushed(BlockData* block_data, Status status);
  void FlushDone(Status s);

  const SliceDataContext slice_data_context_;
  VFSHub* vfs_hub_{nullptr};
  const uint64_t slice_id_;
  const uint64_t block_count_{0};

  std::atomic_uint64_t flush_block_data_count_{0};

  std::mutex mutex_;
  // block_index -> BlockData, this should be immutable
  std::map<uint64_t, BlockDataUPtr> block_datas_;
  StatusCallback cb_;
  Status status_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_SLICE_FLUSH_TASK_H_
