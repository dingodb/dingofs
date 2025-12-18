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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_ACCESSER_COMMON_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_ACCESSER_COMMON_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "common/blockaccess/files/file_common.h"
#include "common/blockaccess/rados/rados_common.h"
#include "common/blockaccess/s3/s3_common.h"
#include "common/options/blockaccess.h"
#include "common/status.h"

namespace dingofs {
namespace blockaccess {

struct BlockAccesserThrottleOptions {
  uint64_t maxAsyncRequestInflightBytes{0};
  uint64_t iopsTotalLimit{0};
  uint64_t iopsReadLimit{0};
  uint64_t iopsWriteLimit{0};
  uint64_t bpsTotalMB{0};
  uint64_t bpsReadMB{0};
  uint64_t bpsWriteMB{0};
};

inline void InitBlockAccesserThrottleOptions(
    BlockAccesserThrottleOptions* options) {
  options->iopsTotalLimit = FLAGS_iops_total_limit;
  options->iopsReadLimit = FLAGS_iops_read_limit;
  options->iopsWriteLimit = FLAGS_iops_write_limit;
  options->bpsTotalMB = FLAGS_io_bandwidth_total_mb;
  options->bpsReadMB = FLAGS_io_bandwidth_read_mb;
  options->bpsWriteMB = FLAGS_io_bandwidth_write_mb;
  options->maxAsyncRequestInflightBytes = FLAGS_io_max_inflight_async_bytes;
}

enum AccesserType : uint8_t {
  kS3 = 0,
  kRados = 1,
  kLocalFile = 2,
};

inline std::string AccesserType2Str(const AccesserType& type) {
  switch (type) {
    case kS3:
      return "S3";
    case kRados:
      return "Rados";
    case kLocalFile:
      return "LocalFile";
    default:
      return "Unknown";
  }
}

struct BlockAccessOptions {
  AccesserType type;
  S3Options s3_options;
  RadosOptions rados_options;
  LocalFileOptions file_options;
  BlockAccesserThrottleOptions throttle_options;
};

// TODO: refact this use one struct
struct GetObjectAsyncContext;
struct PutObjectAsyncContext;

using GetObjectAsyncContextSPtr = std::shared_ptr<GetObjectAsyncContext>;
using PutObjectAsyncContextSPtr = std::shared_ptr<PutObjectAsyncContext>;

using PutObjectAsyncCallBack =
    std::function<void(const PutObjectAsyncContextSPtr&)>;

struct PutObjectAsyncContext {
  uint64_t start_time;

  std::string key;
  const char* buffer;
  size_t buffer_size;

  Status status;

  uint32_t retry;
  PutObjectAsyncCallBack cb;
};

using GetObjectAsyncCallBack =
    std::function<void(const GetObjectAsyncContextSPtr&)>;

struct GetObjectAsyncContext {
  uint64_t start_time;

  std::string key;
  char* buf{nullptr};
  off_t offset;
  size_t len;

  Status status;
  size_t actual_len;

  uint32_t retry;
  GetObjectAsyncCallBack cb;
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_ACCESSER_COMMON_H_
