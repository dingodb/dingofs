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

#ifndef DINGOFS_SRC_TRACE_CONTEXT_H_
#define DINGOFS_SRC_TRACE_CONTEXT_H_

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_format.h>
#include <butil/time.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/options/client.h"
#include "utils/time.h"

namespace dingofs {

class SpanScope;
using SpanScopeSPtr = std::shared_ptr<SpanScope>;

struct Context {
  std::string session_id;  // modify session id

  uint64_t start_time_ns{0};

  bool inner_req{false};

  bool retry{true};
  bool timeout_retry{true};

  // Caller uid carried to MDS via proto Context.uid. Currently only used by
  // root-only paths (manual trash cleanup inside .trash/<bucket>/). 0 means
  // "root or unset" — paths that strictly enforce root must ensure the value
  // was explicitly populated by the FUSE entry point (fuse_req_ctx->uid).
  uint32_t uid{0};

  std::string reason;  // reason for the request, used for log and trace

  absl::InlinedVector<std::pair<const char*, uint64_t>, 8> latency_trace;

  std::weak_ptr<SpanScope> trace_span;

  const std::string& SessionID() const { return session_id; }  // session id

  SpanScopeSPtr GetTraceSpan() const { return trace_span.lock(); }

  void SetTraceSpan(SpanScopeSPtr trace_span_ptr) {
    trace_span = trace_span_ptr;
  }

  void AddLatencyTrace(const char* label, uint64_t latency_us) {
    if (!client::FLAGS_vfs_enable_latency_trace) return;

    latency_trace.emplace_back(label, latency_us);
  }

  std::string ToLatencyTraceStr() const {
    if (!client::FLAGS_vfs_enable_latency_trace) return "";

    std::string result;
    result.reserve(latency_trace.size() * 32);
    for (const auto& entry : latency_trace) {
      result += absl::StrFormat("%s:%llu;", entry.first, entry.second);
    }
    return result;
  }

  Context(const std::string& session) {
    session_id =
        session.empty() ? std::to_string(utils::TimestampNs()) : session;
    start_time_ns = utils::TimestampNs();
  }
};

using ContextSPtr = std::shared_ptr<Context>;

}  // namespace dingofs

#endif  // DINGOFS_SRC_TRACE_CONTEXT_H_
