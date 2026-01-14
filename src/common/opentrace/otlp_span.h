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

#ifndef DINGOFS_COMMON_OPENTRACE_OTLP_SPAN_H_
#define DINGOFS_COMMON_OPENTRACE_OTLP_SPAN_H_

#include <memory>
#include <string>

#include "common/opentrace/type.h"
#include "opentelemetry/trace/span.h"

namespace dingofs {

using SpanContext = opentelemetry::trace::SpanContext;
using SpanSPtr = nostd::shared_ptr<trace::Span>;

class OtlpSpan {
 public:
  OtlpSpan(nostd::shared_ptr<trace::Span> span) : span_(span) {}

  ~OtlpSpan() = default;

  std::shared_ptr<SpanContext> GetContext() const;

  void AddAttribute(const std::string& key, const std::string& value);

  void AddEvent(const std::string& name);

  void SetStatus(bool ok, const std::string& msg);

  std::string GetTraceID();

  std::string GetSpanID();

  void End();

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPENTRACE_OTLP_SPAN_H_