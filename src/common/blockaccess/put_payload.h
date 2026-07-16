/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2026-07-16
 * Author: DingoFS Authors
 */

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_PUT_PAYLOAD_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_PUT_PAYLOAD_H_

#include <glog/logging.h>

#include <cstddef>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

namespace dingofs::blockaccess {

struct PayloadSegment {
  const char* data;
  size_t size;
};

// Immutable, non-owning description of a segmented upload payload. Callers
// must keep the referenced bytes alive and unchanged until Put returns or the
// AsyncPut callback starts.
class PutPayload {
 public:
  static PutPayload Build(std::vector<PayloadSegment> segments) {
    CHECK(!segments.empty()) << "empty block payload is not supported";

    size_t total = 0;
    for (const auto& segment : segments) {
      CHECK_NOTNULL(segment.data);
      CHECK_GT(segment.size, 0) << "block payload has an empty segment";
      CHECK_LE(segment.size, std::numeric_limits<size_t>::max() - total)
          << "payload size overflow";
      total += segment.size;
    }

    return PutPayload(std::make_shared<const Rep>(std::move(segments), total));
  }

  const std::vector<PayloadSegment>& Segments() const { return rep_->segments; }
  size_t Size() const { return rep_->size; }
  size_t SegmentCount() const { return rep_->segments.size(); }

 private:
  struct Rep {
    Rep(std::vector<PayloadSegment> input, size_t total)
        : segments(std::move(input)), size(total) {}

    const std::vector<PayloadSegment> segments;
    const size_t size;
  };

  explicit PutPayload(std::shared_ptr<const Rep> rep) : rep_(std::move(rep)) {
    CHECK_NOTNULL(rep_.get());
  }

  std::shared_ptr<const Rep> rep_;
};

}  // namespace dingofs::blockaccess

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_PUT_PAYLOAD_H_
