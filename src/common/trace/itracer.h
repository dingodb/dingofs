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

#ifndef DINGOFS_COMMON_TRACE_ITRACER_H_
#define DINGOFS_COMMON_TRACE_ITRACER_H_

#include <memory>

#include "common/trace/context.h"
#include "common/trace/itrace_span.h"

namespace dingofs {

class ITracer {
 public:
  virtual ~ITracer() = default;

  virtual std::unique_ptr<ITraceSpan> StartSpan(const std::string& module,
                                                const std::string& name) = 0;

  virtual std::unique_ptr<ITraceSpan> StartSpanWithParent(
      const std::string& module, const std::string& name,
      const ITraceSpan& parent) = 0;

  virtual std::unique_ptr<ITraceSpan> StartSpanWithContext(
      const std::string& module, const std::string& name, ContextSPtr ctx) = 0;

  void EndSpan(const ITraceSpan& span);
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_ITRACER_H_
