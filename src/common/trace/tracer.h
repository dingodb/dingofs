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

#ifndef DINGOFS_COMMON_TRACE_TRACER_H_
#define DINGOFS_COMMON_TRACE_TRACER_H_

#include <butil/time.h>

#include <memory>

#include "common/trace/always_on_sampler.h"
#include "common/trace/context.h"
#include "common/trace/itrace_exporter.h"
#include "common/trace/itrace_sampler.h"
#include "common/trace/itracer.h"

namespace dingofs {

class Tracer : public ITracer {
 public:
  Tracer(std::unique_ptr<ITraceExporter> exporter)
      : exporter_(std::move(exporter)),
        sampler_(std::make_unique<AlwaysOnSampler>()) {}

  std::unique_ptr<ITraceSpan> StartSpan(const std::string& module,
                                        const std::string& name) override;

  std::unique_ptr<ITraceSpan> StartSpanWithParent(
      const std::string& module, const std::string& name,
      const ITraceSpan& parent) override;

  std::unique_ptr<ITraceSpan> StartSpanWithContext(const std::string& module,
                                                   const std::string& name,
                                                   ContextSPtr ctx) override;

  void EndSpan(const ITraceSpan& span);

 private:
  std::unique_ptr<ITraceExporter> exporter_;
  std::unique_ptr<ITraceSampler> sampler_;
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_TRACE_TRACER_H_
