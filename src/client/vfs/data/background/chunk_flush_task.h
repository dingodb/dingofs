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

#ifndef DINGODB_CLIENT_VFS_DATA_CHUNK_FLUSH_TASK_H_
#define DINGODB_CLIENT_VFS_DATA_CHUNK_FLUSH_TASK_H_

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <sstream>

#include "client/vfs/data/slice/slice_data.h"
#include "common/callback.h"

namespace dingofs {
namespace client {
namespace vfs {

class ChunkFlushTask {
 public:
  explicit ChunkFlushTask(
      uint64_t ino, uint64_t index, uint64_t chunk_flush_id,
      std::map<uint64_t, std::unique_ptr<SliceData>> flush_slices)
      : ino_(ino),
        chunk_index_(index),
        chunk_flush_id(chunk_flush_id),
        flush_slices_(std::move(flush_slices)) {}

  ~ChunkFlushTask() {
    VLOG(4) << fmt::format("chunk_flush_task: {} destroy", UUID());
  }

  void RunAsync(StatusCallback cb);

  void GetCommitSlices(std::vector<Slice>& slices) const {
    for (const auto& [seq, slice_data] : flush_slices_) {
      slice_data->GetCommitSlices(slices);
    }
  }

  uint64_t GetFlushSeqId() const { return chunk_flush_id; }

  std::string UUID() const {
    return fmt::format("chunk_flush_task-{}-{}-{}", chunk_flush_id, ino_,
                       chunk_index_);
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << "{ uuid: " << UUID() << ", slices_size: " << flush_slices_.size()
        << " }";
    return oss.str();
  }

 private:
  void SliceFlushed(uint64_t slice_seq, Status s);

  const uint64_t ino_{0};
  const uint64_t chunk_index_{0};
  const uint64_t chunk_flush_id;
  const std::map<uint64_t, std::unique_ptr<SliceData>> flush_slices_;

  std::atomic_uint64_t flusing_slice_{0};

  mutable std::mutex mutex_;
  StatusCallback cb_;
  Status status_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_CHUNK_FLUSH_TASK_H_
