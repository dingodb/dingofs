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

#include "client/vfs/data/background/chunk_flush_task.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <mutex>

#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

void ChunkFlushTask::SliceFlushed(uint64_t slice_seq, Status s) {
  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "Fail flush slice_seq: {} in chunk_flush_task: {}, status: {}",
        slice_seq, ToString(), s.ToString());
  }

  {
    std::lock_guard<std::mutex> lg(mutex_);

    auto it = flush_slices_.find(slice_seq);
    CHECK(it != flush_slices_.end()) << fmt::format(
        "Chunk::SliceFlushed slice_seq: {} not found in task: {}, ", slice_seq,
        ToString());

    CHECK(!it->second->IsFlushed());
    it->second->SetFlushed();

    if (!s.ok()) {
      if (status_.ok()) {
        // only save the first error
        status_ = s;
      }
    }
  }

  if (flusing_slice_.fetch_sub(1, std::memory_order_relaxed) == 1) {
    // all slices flushed
    StatusCallback cb;
    Status tmp;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      cb.swap(cb_);
      tmp = status_;
    }

    cb(tmp);

    VLOG(4) << fmt::format("End chunk_flush_task: {} status: {}", ToString(),
                           tmp.ToString());
  }
}

void ChunkFlushTask::RunAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("Start chunk_flush_task: {} flush_slices size: {}",
                         ToString(), flush_slices_.size());

  if (flush_slices_.empty()) {
    LOG(INFO) << fmt::format(
        "End chunk_flush_task: {} because no slices to flush, return directly",
        ToString());
    cb(Status::OK());
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    cb_.swap(cb);
  }

  flusing_slice_.store(flush_slices_.size(), std::memory_order_relaxed);
  CHECK_GT(flusing_slice_.load(), 0);

  for (const auto& seq_slice : flush_slices_) {
    int64_t seq = seq_slice.first;
    SliceData* slice = seq_slice.second.get();

    VLOG(4) << fmt::format(
        "Chunk::FlushAsync chunk_flush_task: {} will flush slice_seq: {}, "
        "slice: {}",
        ToString(), seq, slice->UUID());

    slice->FlushAsync([this, seq](auto&& ph1) {
      SliceFlushed(seq, std::forward<decltype(ph1)>(ph1));
    });
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
