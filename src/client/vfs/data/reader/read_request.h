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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_READ_REQUEST_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_READ_REQUEST_H_

#include <fmt/format.h>
#include <glog/logging.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

#include "client/vfs/data/common/common.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace client {
namespace vfs {

enum ReadRequestState : uint8_t {
  kNew = 0,
  kBusy,
  kRefresh,
  kReady,
  kInvalid,
};

static std::string ReadRequestStateToString(ReadRequestState state) {
  switch (state) {
    case kNew:
      return "New";
    case kBusy:
      return "Busy";
    case kRefresh:
      return "Refresh";
    case kReady:
      return "Ready";
    case kInvalid:
      return "Invalid";
    default:
      return fmt::format("Unknown-{}", static_cast<uint8_t>(state));
  }
}

enum TransitionReason : uint8_t {
  kCleanUp = 0,
  kInvalidate,
  kReading,
  kReadDone,
};

static std::string TransitionReasonToString(TransitionReason reason) {
  switch (reason) {
    case kCleanUp:
      return "CleanUp";
    case kInvalidate:
      return "Invalidate";
    case kReading:
      return "Reading";
    case kReadDone:
      return "ReadDone";
    default:
      return fmt::format("Unknown-{}", static_cast<uint8_t>(reason));
  }
}

struct ReadRequest {
  const ChunkReq req;

  mutable std::mutex mutex;
  std::condition_variable cv;
  int32_t readers{0};
  ReadRequestState state;
  Status status;
  int64_t access_sec;
  IOBuffer buffer;

  explicit ReadRequest(uint64_t ino, int64_t chunk_index, int64_t chunk_offset,
                       FileRange frange)
      : req(ino, chunk_index, chunk_offset, frange) {}

  void IncReaderUnlock() {
    CHECK_GE(readers, 0);
    ++readers;
  }

  void IncReader() {
    std::unique_lock<std::mutex> lock(mutex);
    IncReaderUnlock();
  }

  void DecReader() {
    std::unique_lock<std::mutex> lock(mutex);
    CHECK_GT(readers, 0);
    --readers;
  }

  void ToStateUnLock(ReadRequestState new_state, TransitionReason reason);

  uint64_t ReqId() const { return req.req_id; }

  bool Overlaps(const FileRange& other) const {
    return req.frange.Overlaps(other);
  }

  std::string UUID() const { return req.UUID(); }

  std::string ToStringUnlock() const;

  std::string ToString() const;
};

using ReadRequestSptr = std::shared_ptr<ReadRequest>;

struct PartialReadRequest {
  ReadRequestSptr req;
  int64_t offset{0};
  int64_t len{0};

  std::string ToString() const;

  std::string ToStringUnlock() const;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_READ_REQUEST_H_
