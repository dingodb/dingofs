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

#include "common/trace/tracer.h"

#include <butil/time.h>

#include "common/trace/context.h"
#include "common/trace/trace_span.h"
#include "common/trace/utils.h"

namespace dingofs {

std::unique_ptr<ITraceSpan> Tracer::StartSpan(const std::string& module,
                                              const std::string& name) {
  auto context =
      std::make_shared<Context>(module, GenerateTraceId(), GenerateSpanId());
  return std::unique_ptr<ITraceSpan>(new TraceSpan(this, name, context));
}

std::unique_ptr<ITraceSpan> Tracer::StartSpanWithParent(
    const std::string& module, const std::string& name,
    const ITraceSpan& parent) {
  const auto& parent_ctx = parent.GetContext();
  auto span_id = GenerateSpanId();
  auto context = std::make_shared<Context>(module, parent_ctx->trace_id,
                                           span_id, parent_ctx->span_id);

  return std::unique_ptr<TraceSpan>(new TraceSpan(this, name, context));
}

std::unique_ptr<ITraceSpan> Tracer::StartSpanWithContext(
    const std::string& module, const std::string& name, ContextSPtr ctx) {
  auto span_id = GenerateSpanId();
  auto context =
      std::make_shared<Context>(module, ctx->trace_id, span_id, ctx->span_id);

  return std::unique_ptr<TraceSpan>(new TraceSpan(this, name, context));
}

void Tracer::EndSpan(const ITraceSpan& span) {
  if (sampler_->ShouldSample()) {
    exporter_->Export(span);
  }
}

}  // namespace dingofs
