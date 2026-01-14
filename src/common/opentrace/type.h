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

#include "opentelemetry/context/context.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "opentelemetry/trace/span_context.h"

namespace dingofs {

namespace nostd = opentelemetry::nostd;
namespace trace = opentelemetry::trace;

using SpanContext = opentelemetry::trace::SpanContext;
using SpanSPtr = nostd::shared_ptr<trace::Span>;

inline std::string ToString(trace::TraceId const& trace_id) {
  constexpr int kSize = trace::TraceId::kSize * 2;
  std::string buffer(kSize, '\0');
  trace_id.ToLowerBase16(buffer);
  return buffer;
}

inline std::string ToString(trace::SpanId const& span_id) {
  constexpr int kSize = trace::SpanId::kSize * 2;
  std::string buffer(kSize, '\0');
  span_id.ToLowerBase16(buffer);
  return buffer;
}

}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPENTRACE_TYPE_H_
