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

#include <absl/strings/str_format.h>
#include <butil/time.h>

#include <string>

#include "utils/time.h"

namespace dingofs {

class SpanScope;
using SpanScopeSptr = std::shared_ptr<SpanScope>;

struct Context {
  std::string module;
  std::string trace_id;  // modify session id
  std::string span_id;   // delete
  // when root Span, parent_span_id is empty
  std::string parent_span_id;  // delete
  // whether hit local cache
  bool hit_cache{false};
  bool is_amend{false};

  bool need_cache{false};

  uint64_t start_time_ns{0};  // delete

  std::weak_ptr<SpanScope> trace_span;

  const std::string& TraceId() const { return trace_id; }  // session id

  SpanScopeSptr GetTraceSpan() { return trace_span.lock(); }

  void SetTraceSpan(SpanScopeSptr trace_span_ptr) {
    trace_span = trace_span_ptr;
  }

  Context(std::string module, std::string trace, std::string span,
          std::string parent = "")
      : module(std::move(module)),
        trace_id(std::move(trace)),
        span_id(std::move(span)),
        parent_span_id(std::move(parent)) {
    start_time_ns = utils::TimestampNs();
  }
};

using ContextSPtr = std::shared_ptr<Context>;

}  // namespace dingofs

#endif  // DINGOFS_SRC_TRACE_CONTEXT_H_
