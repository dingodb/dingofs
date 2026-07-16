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

#include <glog/logging.h>

#include <algorithm>
#include <any>
#include <cstring>
#include <ios>
#include <limits>
#include <memory>
#include <streambuf>
#include <string>

#include "aws/core/client/AsyncCallerContext.h"
#include "aws/core/utils/memory/AWSMemory.h"
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

struct AwsDeleteObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::any request;
  DeleteObjectAsyncContextSPtr user_ctx;
};
using AwsDeleteObjectAsyncContextSPtr =
    std::shared_ptr<AwsDeleteObjectAsyncContext>;

struct AwsDeleteObjectsAsyncContext : public Aws::Client::AsyncCallerContext {
  std::any request;
  BatchDeleteObjectAsyncContextSPtr user_ctx;
};
using AwsDeleteObjectsAsyncContextSPtr =
    std::shared_ptr<AwsDeleteObjectsAsyncContext>;

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

class SegmentedStreamBuf : public std::streambuf {
 public:
  explicit SegmentedStreamBuf(PutPayload payload)
      : payload_(std::move(payload)) {
    CHECK_LE(payload_.Size(),
             static_cast<size_t>(std::numeric_limits<off_type>::max()));
  }

 protected:
  int_type underflow() override {
    if (position_ >= payload_.Size()) {
      return traits_type::eof();
    }
    auto [segment, offset] = Locate(position_);
    return traits_type::to_int_type(segment->data[offset]);
  }

  int_type uflow() override {
    int_type value = underflow();
    if (!traits_type::eq_int_type(value, traits_type::eof())) {
      ++position_;
    }
    return value;
  }

  std::streamsize xsgetn(char* output, std::streamsize count) override {
    if (count <= 0 || position_ >= payload_.Size()) {
      return 0;
    }

    size_t remaining =
        std::min(static_cast<size_t>(count), payload_.Size() - position_);
    const size_t requested = remaining;
    while (remaining > 0) {
      auto [segment, offset] = Locate(position_);
      size_t length = std::min(remaining, segment->size - offset);
      std::memcpy(output, segment->data + offset, length);
      output += length;
      position_ += length;
      remaining -= length;
    }
    return static_cast<std::streamsize>(requested);
  }

  pos_type seekoff(off_type offset, std::ios_base::seekdir direction,
                   std::ios_base::openmode mode) override {
    if ((mode & std::ios_base::in) == 0) {
      return pos_type(off_type(-1));
    }
    off_type base = 0;
    if (direction == std::ios_base::cur) {
      base = static_cast<off_type>(position_);
    } else if (direction == std::ios_base::end) {
      base = static_cast<off_type>(payload_.Size());
    } else if (direction != std::ios_base::beg) {
      return pos_type(off_type(-1));
    }
    const off_type end = static_cast<off_type>(payload_.Size());
    if (offset < -base || offset > end - base) {
      return pos_type(off_type(-1));
    }
    off_type next = base + offset;
    position_ = static_cast<size_t>(next);
    return pos_type(next);
  }

  pos_type seekpos(pos_type position, std::ios_base::openmode mode) override {
    return seekoff(static_cast<off_type>(position), std::ios_base::beg, mode);
  }

 private:
  std::pair<const PayloadSegment*, size_t> Locate(size_t position) {
    if (position < cursor_segment_start_) {
      cursor_segment_ = 0;
      cursor_segment_start_ = 0;
    }

    const auto& segments = payload_.Segments();
    while (cursor_segment_ < segments.size()) {
      const auto& segment = segments[cursor_segment_];
      if (position < cursor_segment_start_ + segment.size) {
        return {&segment, position - cursor_segment_start_};
      }
      cursor_segment_start_ += segment.size;
      ++cursor_segment_;
    }
    LOG(FATAL) << "payload position out of bounds";
    return {nullptr, 0};
  }

  PutPayload payload_;
  size_t position_{0};
  size_t cursor_segment_{0};
  size_t cursor_segment_start_{0};
};

class SegmentedIOStream : public Aws::IOStream {
 public:
  explicit SegmentedIOStream(PutPayload payload)
      : Aws::IOStream(new SegmentedStreamBuf(std::move(payload))) {}

  ~SegmentedIOStream() override { delete rdbuf(); }
};

template <typename PutObjectRequest>
void SetPutObjectPayload(PutObjectRequest* request, const PutPayload& payload) {
  CHECK_NOTNULL(request);
  CHECK_LE(payload.Size(),
           static_cast<size_t>(std::numeric_limits<long long>::max()));
  request->SetContentLength(static_cast<long long>(payload.Size()));
  request->SetBody(
      Aws::MakeShared<SegmentedIOStream>(AWS_ALLOCATE_TAG, payload));
}

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
