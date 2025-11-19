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

#ifndef DINGOFS_COMMON_TRACE_SPAN_SCOPE_H_
#define DINGOFS_COMMON_TRACE_SPAN_SCOPE_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "butil/status.h"
#include "common/opentrace/opentelemetry/type.h"
#include "common/opentrace/span.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "glog/logging.h"
#include "opentelemetry/trace/span_context.h"

namespace dingofs {

class TraceManager;

class SpanScope : public std::enable_shared_from_this<SpanScope> {
 public:
  SpanScope(std::shared_ptr<Span> inner_span, std::shared_ptr<SpanScope> parent,
            ContextSPtr ctx)
      : inner_span_(std::move(inner_span)),
        parent_(parent),
        ended_(false),
        context_(std::move(ctx)) {}

  static std::shared_ptr<SpanScope> Create(std::shared_ptr<TraceManager> mgr,
                                           const std::string& name);

  static std::shared_ptr<SpanScope> Create(std::shared_ptr<TraceManager> mgr,
                                           const std::string& name,
                                           const std::string& trace_id,
                                           const std::string& span_id);

  static std::shared_ptr<SpanScope> CreateChild(
      std::shared_ptr<TraceManager> mgr, const std::string& name,
      std::shared_ptr<SpanScope> parent);

  ~SpanScope() { End(); };

  void End() {
    if (ended_.exchange(true)) return;
    inner_span_->End();
  }

  std::shared_ptr<SpanContext> GetTraceContext() const {
    return inner_span_->GetContext();
  }

  void SetTraceSpan() { context_->SetTraceSpan(shared_from_this()); }

  ContextSPtr GetContext() const { return context_; }

  void AddAttribute(const std::string& key, const std::string& value) {
    inner_span_->AddAttribute(key, value);
  }

  void AddEvent(const std::string& name) { inner_span_->AddEvent(name); }

  void SetStatus(const Status& status) {
    inner_span_->SetStatus(status.ok(), status.ToString());
  }

  void SetStatus(butil::Status const& status) {
    inner_span_->SetStatus(status.ok(), status.error_str());
  }

  std::string GetTraceID() { return inner_span_->GetTraceID(); }

  std::string GetSpanID() { return inner_span_->GetSpanID(); }

 private:
  std::shared_ptr<Span> inner_span_;
  std::shared_ptr<SpanScope> parent_;
  std::atomic<bool> ended_;
  ContextSPtr context_;
};

using SpanScopeSptr = std::shared_ptr<SpanScope>;
}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_SPAN_SCOPE_H_