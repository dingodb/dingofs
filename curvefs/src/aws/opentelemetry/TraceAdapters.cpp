
// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "curvefs/src/aws/opentelemetry/TraceAdapters.h"

namespace curvefs {
namespace aws {
namespace opentelemetry {

using namespace smithy::components::tracing;

static const char* ALLOC_TAG = "OTEL_ADAPTER";

std::shared_ptr<TraceSpan> OtelTracerAdapter::CreateSpan(
    Aws::String name, const Aws::Map<Aws::String, Aws::String>& attributes,
    SpanKind span_kind) {
  ::opentelemetry::trace::StartSpanOptions span_options;
  span_options.kind = ConvertSpanKind(span_kind);
  span_options.parent = ::opentelemetry::context::RuntimeContext::GetCurrent();

  auto otel_span = otelTracer_->StartSpan(name, attributes, {}, span_options);
  auto scope = otelTracer_->WithActiveSpan(otel_span);
  return Aws::MakeShared<OtelSpanAdapter>(ALLOC_TAG, name, otel_span,
                                          std::move(scope));
}

::opentelemetry::trace::SpanKind OtelTracerAdapter::ConvertSpanKind(
    SpanKind status) {
  if (status == SpanKind::SERVER) {
    return ::opentelemetry::trace::SpanKind::kServer;
  } else if (status == SpanKind::INTERNAL) {
    return ::opentelemetry::trace::SpanKind::kInternal;
  } else if (status == SpanKind::CLIENT) {
    return ::opentelemetry::trace::SpanKind::kClient;
  }
  return ::opentelemetry::trace::SpanKind::kClient;
}

OtelSpanAdapter::~OtelSpanAdapter() { end(); }

void OtelSpanAdapter::emitEvent(
    Aws::String name, const Aws::Map<Aws::String, Aws::String>& attributes) {
  otelSpan_->AddEvent(name, attributes);
}

void OtelSpanAdapter::setStatus(
    smithy::components::tracing::TraceSpanStatus status) {
  otelSpan_->SetStatus(ConvertStatusCode(status));
}

void OtelSpanAdapter::setAttribute(Aws::String key, Aws::String value) {
  otelSpan_->SetAttribute(key, value);
}

void OtelSpanAdapter::end() { otelSpan_->End(); }

::opentelemetry::trace::StatusCode OtelSpanAdapter::ConvertStatusCode(
    TraceSpanStatus status) {
  if (status == TraceSpanStatus::OK) {
    return ::opentelemetry::trace::StatusCode::kOk;
  } else if (status == TraceSpanStatus::FAULT) {
    return ::opentelemetry::trace::StatusCode::kError;
  } else if (status == TraceSpanStatus::UNSET) {
    return ::opentelemetry::trace::StatusCode::kUnset;
  }
  return ::opentelemetry::trace::StatusCode::kOk;
}

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs