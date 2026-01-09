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
#include "common/opentrace/otlp_span.h"
#include "common/opentrace/tracer.h"
#include "common/opentrace/type.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {

class TraceManager;

class SpanScope;
using SpanScopeSPtr = std::shared_ptr<SpanScope>;
class SpanScope : public std::enable_shared_from_this<SpanScope> {
 public:
  SpanScope(OtlpSpan span, std::shared_ptr<SpanScope> parent, ContextSPtr ctx)
      : inner_span_(std::move(span)),
        parent_(parent),
        ended_(false),
        context_(std::move(ctx)) {}

  static SpanScopeSPtr Create(OpenTeleMetryTracer& tracer,
                              const std::string& name);

  static SpanScopeSPtr Create(OpenTeleMetryTracer& tracer,
                              const std::string& name,
                              const std::string& trace_id,
                              const std::string& span_id);

  static SpanScopeSPtr CreateChild(OpenTeleMetryTracer& tracer,
                                   const std::string& name,
                                   SpanScopeSPtr parent);

  ~SpanScope() { End(); };

  static std::string GetTraceID(SpanScopeSPtr span) {
    if (span) {
      return span->GetTraceID();
    }
    return "";
  }

  static std::string GetSpanID(SpanScopeSPtr span) {
    if (span) {
      return span->GetSpanID();
    }
    return "";
  }

  static void AddAttribute(SpanScopeSPtr span, const std::string& key,
                           const std::string& value) {
    if (span) {
      span->AddAttribute(key, value);
    }
  }

  static void AddEvent(SpanScopeSPtr span, const std::string& name) {
    if (span) {
      span->AddEvent(name);
    }
  }

  static void SetStatus(SpanScopeSPtr span, const Status& status) {
    if (span) {
      span->SetStatus(status);
    }
  }

  static void SetStatus(SpanScopeSPtr span, butil::Status const& status) {
    if (span) {
      span->SetStatus(status);
    }
  }

  static std::shared_ptr<SpanContext> GetTraceContext(SpanScopeSPtr span) {
    if (span) {
      return span->GetTraceContext();
    }
    return nullptr;
  }

  static ContextSPtr GetContext(SpanScopeSPtr span) {
    if (span) {
      return span->GetContext();
    }
    return std::make_shared<Context>("");
  }

  static std::string GetSessionID(SpanScopeSPtr span) {
    if (span) {
      return span->context_->SessionID();
    }
    return std::to_string(utils::TimestampNs());
  }

  static void End(SpanScopeSPtr span) {
    if (span) {
      span->End();
    }
  }

  static void SetTraceSpan(SpanScopeSPtr span) {
    if (span) {
      span->SetTraceSpan();
    }
  }

 private:
  void End() {
    if (ended_.exchange(true)) return;
    inner_span_.End();
  }

  void SetTraceSpan() { context_->SetTraceSpan(shared_from_this()); }

  ContextSPtr GetContext() const { return context_; }

  std::shared_ptr<SpanContext> GetTraceContext() {
    return inner_span_.GetContext();
  }

  void AddAttribute(const std::string& key, const std::string& value) {
    inner_span_.AddAttribute(key, value);
  }

  void AddEvent(const std::string& name) { inner_span_.AddEvent(name); }

  void SetStatus(const Status& status) {
    inner_span_.SetStatus(status.ok(), status.ToString());
  }

  void SetStatus(butil::Status const& status) {
    inner_span_.SetStatus(status.ok(), status.error_str());
  }

  std::string GetTraceID() { return inner_span_.GetTraceID(); }

  std::string GetSpanID() { return inner_span_.GetSpanID(); }

  OtlpSpan inner_span_;
  SpanScopeSPtr parent_;
  std::atomic<bool> ended_;
  ContextSPtr context_;
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_SPAN_SCOPE_H_