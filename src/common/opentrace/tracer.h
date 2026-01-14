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

#ifndef DINGOFS_COMMON_OPENTRACE_TRACER_H_
#define DINGOFS_COMMON_OPENTRACE_TRACER_H_

#include <memory>

#include "common/opentrace/otlp_span.h"
#include "common/opentrace/type.h"
#include "opentelemetry/trace/tracer.h"

namespace dingofs {

using SpanContext = opentelemetry::trace::SpanContext;

class OpenTeleMetryTracer {
 public:
  OpenTeleMetryTracer(const std::string& service_name,
                      const std::string& otlp_export_endpoint,
                      uint32_t export_thread_num,
                      const std::string& commit_hash,
                      const std::string& version)
      : service_name_(service_name),
        otlp_export_endpoint_{otlp_export_endpoint},
        max_export_thread_(export_thread_num),
        commit_hash_(commit_hash),
        version_(version) {}

  bool Init();

  void Stop();

  static std::shared_ptr<OpenTeleMetryTracer> New(
      const std::string& service_name, const std::string& otlp_export_endpoint,
      uint32_t export_thread_num, const std::string& commit_hash = "",
      const std::string& version = "") {
    return std::make_shared<OpenTeleMetryTracer>(
        service_name, otlp_export_endpoint, export_thread_num, commit_hash,
        version);
  }

  OtlpSpan MakeSpan(const std::string& name);

  OtlpSpan MakeSpan(const std::string& name, const SpanContext& span_context);

  OtlpSpan MakeSpan(const std::string& name, const std::string& trace_id,
                    const std::string& span_id);

 private:
  std::string service_name_;
  std::string otlp_export_endpoint_;
  std::string commit_hash_;
  std::string version_;
  nostd::shared_ptr<trace::Tracer> tracer_;
  uint32_t max_export_thread_;
};
using OpenTeleMetryTracerSPtr = std::shared_ptr<OpenTeleMetryTracer>;

}  // namespace dingofs

#endif  // DINGOFS_COMMON_OPENTRACE_TRACER_H_
