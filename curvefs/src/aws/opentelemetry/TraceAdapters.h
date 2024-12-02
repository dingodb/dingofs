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

#ifndef SRC_AWS_S3_OPENTELEMETRY_TRACE_ADAPTER_H_
#define SRC_AWS_S3_OPENTELEMETRY_TRACE_ADAPTER_H_

#include <memory>
#include <utility>

#include "opentelemetry/nostd/shared_ptr.h"
#include "opentelemetry/trace/scope.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/trace/tracer.h"
#include "smithy/tracing/TraceSpan.h"
#include "smithy/tracing/Tracer.h"

namespace curvefs {
namespace aws {
namespace opentelemetry {

using smithy::components::tracing::SpanKind;
using smithy::components::tracing::Tracer;
using smithy::components::tracing::TraceSpan;
using smithy::components::tracing::TraceSpanStatus;

/**
 * A Open Telemetry Implementation of TraceSpan.
 */
class OtelSpanAdapter final : public TraceSpan {
 public:
  explicit OtelSpanAdapter(
      Aws::String name,
      ::opentelemetry::nostd::shared_ptr<::opentelemetry::trace::Span> span,
      ::opentelemetry::trace::Scope scope)
      : TraceSpan(std::move(name)),
        otelSpan_(std::move(span)),
        otelScope_(std::move(scope)) {}

  ~OtelSpanAdapter() override;

  void emitEvent(Aws::String name,
                 const Aws::Map<Aws::String, Aws::String>& attributes) override;

  void setAttribute(Aws::String key, Aws::String value) override;

  void setStatus(TraceSpanStatus status) override;

  void end() override;

 private:
  ::opentelemetry::trace::StatusCode ConvertStatusCode(TraceSpanStatus status);

  ::opentelemetry::nostd::shared_ptr<::opentelemetry::trace::Span> otelSpan_;
  ::opentelemetry::trace::Scope otelScope_;
};

/**
 * A Open Telemetry Implementation of Tracer.
 */
class OtelTracerAdapter final : public Tracer {
 public:
  explicit OtelTracerAdapter(
      ::opentelemetry::nostd::shared_ptr<::opentelemetry::trace::Tracer> tracer)
      : otelTracer_(std::move(tracer)) {}

  std::shared_ptr<TraceSpan> CreateSpan(
      Aws::String name, const Aws::Map<Aws::String, Aws::String>& attributes,
      SpanKind span_kind) override;

 private:
  ::opentelemetry::trace::SpanKind ConvertSpanKind(SpanKind status);

  ::opentelemetry::nostd::shared_ptr<::opentelemetry::trace::Tracer>
      otelTracer_;
};

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs
#endif  // SRC_AWS_S3_OPENTELEMETRY_TRACE_ADAPTER_H_