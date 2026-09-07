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

#ifndef DINGOFS_COMMON_TRACE_TRACE_MANAGER_H_
#define DINGOFS_COMMON_TRACE_TRACE_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "common/options/common.h"
#include "common/trace/span_scope.h"

namespace dingofs {

class SpanScope;
using SpanScopeSPtr = std::shared_ptr<SpanScope>;

class TraceManager {
 public:
  TraceManager();
  ~TraceManager() = default;

  bool Init();

  void Stop();

  OpenTeleMetryTracer& GetTracer() { return tracer_; }

  // Names are string_view so a disabled tracer costs no string construction.
  inline SpanScopeSPtr StartSpan(std::string_view name) {
    if (!FLAGS_enable_trace) {
      return nullptr;
    }

    auto scope = SpanScope::Create(GetTracer(), std::string(name));
    SpanScope::SetTraceSpan(scope);
    return scope;
  }

  inline SpanScopeSPtr StartSpan(std::string_view name,
                                 const std::string& trace_id,
                                 const std::string& span_id) {
    if (!FLAGS_enable_trace) {
      return nullptr;
    }

    auto scope =
        SpanScope::Create(GetTracer(), std::string(name), trace_id, span_id);
    SpanScope::SetTraceSpan(scope);
    return scope;
  }

  inline SpanScopeSPtr StartChildSpan(std::string_view name,
                                      SpanScopeSPtr parent) {
    if (!FLAGS_enable_trace) {
      return nullptr;
    }

    auto scope = SpanScope::CreateChild(GetTracer(), std::string(name), parent);
    SpanScope::SetTraceSpan(scope);
    return scope;
  }

 private:
  OpenTeleMetryTracer tracer_;
};

using TraceManagerSPtr = std::shared_ptr<TraceManager>;
}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_TRACE_MANAGER_H_