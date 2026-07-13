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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_FLUSH_BARRIER_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_FLUSH_BARRIER_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

// Owns the complete upload/flush state machine in one synchronization domain.
// size/start_us have no runtime consumer by design: an attached debugger can
// use them to reconstruct a BlockKey and calculate the age of a stuck upload.
class FlushBarrier {
 public:
  struct BlockInfo {
    uint32_t index;
    uint32_t size;
    uint64_t start_us;
  };

  // tag identifies the owning slice (SliceWriter UUID) in CHECK failures and
  // logs; a bare block index is unattributable among concurrent slices.
  explicit FlushBarrier(std::string tag) : tag_(std::move(tag)) {}

  void TrackStreamingUpload(BlockInfo block);

  // Atomically registers the final upload batch and prevents future upload
  // registration. Returns true when the caller must submit the final batch;
  // false means the flush completed immediately or failed before submission.
  // Completion callbacks are always invoked outside mutex_.
  bool RegisterFinalUploads(const std::vector<BlockInfo>& remaining,
                            StatusCallback callback);

  // Completes one accepted upload. Before RegisterFinalUploads, an empty set
  // is only transient. Once all uploads are registered, the callback that
  // empties the set delivers the unique flush completion.
  void FinishUpload(uint32_t index, const Status& status);

  // Fails orchestration before final upload registration (for example slice-id
  // allocation). Late upload callbacks only erase their entries afterward.
  void AbortFlush(Status status, StatusCallback callback);

  size_t Inflight() const;
  Status FirstError() const;

 private:
  enum class State : uint8_t {
    kAcceptingUploads,
    kAllUploadsRegistered,
    kCompletionDelivered,
  };

  struct Completion {
    Status status;
    StatusCallback callback;
  };

  std::optional<Completion> TakeCompletionUnlocked();
  static void Deliver(std::optional<Completion> completion);

  const std::string tag_;
  mutable std::mutex mutex_;
  State state_{State::kAcceptingUploads};
  Status first_error_;
  StatusCallback callback_;
  std::map<uint32_t, BlockInfo> inflight_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_FLUSH_BARRIER_H_
