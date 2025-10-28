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

#include "common/blockaccess/rados/rados_common.h"
#include "common/blockaccess/s3/s3_common.h"
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
    utils::Configuration* conf, BlockAccesserThrottleOptions* options) {
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsTotalLimit",
                                      &options->iopsTotalLimit));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsReadLimit",
                                      &options->iopsReadLimit));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsWriteLimit",
                                      &options->iopsWriteLimit));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsTotalMB", &options->bpsTotalMB));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsReadMB", &options->bpsReadMB));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsWriteMB", &options->bpsWriteMB));

  if (!conf->GetUInt64Value("s3.maxAsyncRequestInflightBytes",
                            &options->maxAsyncRequestInflightBytes)) {
    LOG(WARNING) << "Not found s3.maxAsyncRequestInflightBytes in conf";
    options->maxAsyncRequestInflightBytes = 0;
  }
}

enum AccesserType : uint8_t {
  kS3 = 0,
  kRados = 1,
};

inline std::string AccesserType2Str(const AccesserType& type) {
  switch (type) {
    case kS3:
      return "S3";
    case kRados:
      return "Rados";
    default:
      return "Unknown";
  }
}

struct BlockAccessOptions {
  AccesserType type;
  S3Options s3_options;
  RadosOptions rados_options;
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
