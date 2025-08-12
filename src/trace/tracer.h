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

#ifndef DINGOFS_SRC_TRACE_TRACER_H_
#define DINGOFS_SRC_TRACE_TRACER_H_

#include <butil/time.h>

#include <memory>

#include "trace/context.h"
#include "trace/sampler.h"
#include "trace/trace_exporter.h"
#include "trace/trace_span.h"

namespace dingofs {

class Tracer {
 public:
  Tracer(std::unique_ptr<TraceExporter> exporter)
      : exporter_(std::move(exporter)),
        sampler_(std::make_unique<AlwaysOnSampler>()) {}

  std::unique_ptr<TraceSpan> StartSpan(const std::string& module,
                                       const std::string& name);

  std::unique_ptr<TraceSpan> StartSpanWithParent(const std::string& module,
                                                 const std::string& name,
                                                 const TraceSpan& parent);

  std::unique_ptr<TraceSpan> StartSpanWithContext(const std::string& module,
                                                  const std::string& name,
                                                  ContextSPtr ctx);

  void EndSpan(const TraceSpan& span);

  void SetSampler(std::unique_ptr<TraceSampler> sampler);

 private:
  std::string GenerateId(size_t length);

  std::unique_ptr<TraceExporter> exporter_;
  std::unique_ptr<TraceSampler> sampler_;
};

}  // namespace dingofs

#endif  // DINGOFS_SRC_TRACE_TRACER_H_
