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

#include "common/opentrace/otlp_span.h"

#include <memory>

#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {

OtlpSpan::~OtlpSpan() {}

void OtlpSpan::SetStatus(bool ok, const std::string& msg) {
  if (ok) {
    span_->SetStatus(trace::StatusCode::kOk);
    return;
  }
  span_->SetStatus(trace::StatusCode::kError, msg);
  span_->SetAttribute("dingofs.error.message", msg);
}

void OtlpSpan::AddAttribute(std::string const& key, std::string const& value) {
  span_->SetAttribute(key, value);
}

void OtlpSpan::AddEvent(std::string const& name) { span_->AddEvent(name); }

std::string OtlpSpan::GetTraceID() {
  auto span_context = span_->GetContext();
  return ToString(span_context.trace_id());
}
std::string OtlpSpan::GetSpanID() {
  auto span_context = span_->GetContext();
  return ToString(span_context.span_id());
}

std::shared_ptr<SpanContext> OtlpSpan::GetContext() const {
  return std::make_shared<SpanContext>(span_->GetContext());
}

void OtlpSpan::End() { span_->End(); }

}  // namespace dingofs