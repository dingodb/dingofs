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

/*
 * Project: DingoFS
 * Created Date: 2025-06-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_COMMON_CONTEXT_H_
#define DINGOFS_SRC_COMMON_CONTEXT_H_

#include <absl/strings/str_format.h>
#include <butil/time.h>

namespace dingofs {

inline std::string NewTraceId() {
  return absl::StrFormat("%lld", butil::cpuwide_time_ns());
}

class Context {
 public:
  Context() : trace_id_(NewTraceId()) {}
  Context(const std::string& trace_id, const std::string& sub_trace_id = "")
      : trace_id_(trace_id), sub_trace_id_(sub_trace_id) {}

  std::string TraceId() const { return trace_id_; }
  std::string SubTraceId() const { return sub_trace_id_; }
  std::string StrTraceId() const {
    if (sub_trace_id_.empty()) {
      return absl::StrFormat("[%s]", trace_id_);
    }
    return absl::StrFormat("[%s:%s]", trace_id_, sub_trace_id_);
  }

 private:
  const std::string trace_id_;
  const std::string sub_trace_id_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_CONTEXT_H_
