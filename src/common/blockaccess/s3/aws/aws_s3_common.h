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

#ifndef DATA_ACCESS_AWS_S3_COMMON_H
#define DATA_ACCESS_AWS_S3_COMMON_H

#include <any>
#include <memory>
#include <string>

#include "aws/core/client/AsyncCallerContext.h"
#include "aws/core/utils/memory/stl/AWSStreamFwd.h"
#include "aws/core/utils/stream/PreallocatedStreamBuf.h"
#include "common/blockaccess/accesser_common.h"
#include "utils/macros.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace dingofs {
namespace blockaccess {
namespace aws {

struct AwsGetObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::any request;
  GetObjectAsyncContextSPtr user_ctx;
};
using AwsGetObjectAsyncContextSPtr = std::shared_ptr<AwsGetObjectAsyncContext>;

struct AwsPutObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::any request;
  PutObjectAsyncContextSPtr user_ctx;
};
using AwsPutObjectAsyncContextSPtr = std::shared_ptr<AwsPutObjectAsyncContext>;

// https://github.com/aws/aws-sdk-cpp/issues/1430
class PreallocatedIOStream : public Aws::IOStream {
 public:
  PreallocatedIOStream(char* buf, size_t size)
      : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(buf), size)) {}

  PreallocatedIOStream(const char* buf, size_t size)
      : PreallocatedIOStream(const_cast<char*>(buf), size) {}

  ~PreallocatedIOStream() override {
    // corresponding new in constructor
    delete rdbuf();
  }
};

// https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Range_requests
inline Aws::String GetObjectRequestRange(uint64_t offset, uint64_t len) {
  CHECK_GT(len, 0);
  auto range = "bytes=" + std::to_string(offset) + "-" +
               std::to_string(offset + len - 1);
  return {range.data(), range.size()};
}

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs

#endif  // DATA_ACCESS_AWS_S3_COMMON_H