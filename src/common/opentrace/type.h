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

#ifndef DINGOFS_COMMON_OPENTRACE_TYPE_H_
#define DINGOFS_COMMON_OPENTRACE_TYPE_H_

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#include <map>
#include <memory>

#include "brpc/controller.h"
#include "butil/status.h"
#include "butil/time.h"
#include "common/status.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/span_context.h"

namespace dingofs {
    
namespace nostd = opentelemetry::nostd;
namespace trace = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace context = opentelemetry::context;
namespace propagation = opentelemetry::context::propagation;
namespace resource = opentelemetry::sdk::resource;
namespace otlp = opentelemetry::exporter::otlp;

using SpanContext = opentelemetry::trace::SpanContext;
using SpanSPtr = nostd::shared_ptr<trace::Span>;

inline std::string ToString(trace::TraceId const& trace_id) {
  constexpr int kSize = trace::TraceId::kSize * 2;
  char trace_id_array[kSize];
  trace_id.ToLowerBase16(trace_id_array);
  return std::string(trace_id_array, kSize);
}

inline std::string ToString(trace::SpanId const& span_id) {
  constexpr int kSize = trace::SpanId::kSize * 2;
  char span_id_array[kSize];
  span_id.ToLowerBase16(span_id_array);
  return std::string(span_id_array, kSize);
}

}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPENTRACE_TYPE_H_
