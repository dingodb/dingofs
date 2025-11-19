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

#ifndef DINGOFS_COMMON_OPENTRACE_OPENTELEMETRY_OTLP_SPAN_H_
#define DINGOFS_COMMON_OPENTRACE_OPENTELEMETRY_OTLP_SPAN_H_

#include <atomic>
#include <memory>
#include <string>

#include "common/opentrace/span.h"
#include "common/opentrace/opentelemetry/type.h"

namespace dingofs {

class OtlpSpan : public Span {
 public:
  OtlpSpan(nostd::shared_ptr<trace::Span> span)
      : span_(span), scope_(trace::Scope(span)) {}

  ~OtlpSpan() override;

  OtlpSpan(const OtlpSpan&) = delete;

  OtlpSpan& operator=(const OtlpSpan&) = delete;

  std::shared_ptr<SpanContext> GetContext() const override;

  void AddAttribute(const std::string& key, const std::string& value) override;

  void AddEvent(const std::string& name) override;

  void SetStatus(bool ok, const std::string& msg) override;

  std::string GetTraceID() override;

  std::string GetSpanID() override;

  void End() override;

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::trace::Scope scope_;
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPENTRACE_OPENTELEMETRY_OTLP_SPAN_H_