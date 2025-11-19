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

#include "common/trace/trace_manager.h"

#include <memory>

#include "common/opentrace/opentelemetry/tracer.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/version.h"

namespace dingofs {

DEFINE_string(trace_service_name, "dingofs", "service name");
DEFINE_string(otlp_export_endpoint, "172.30.14.125:4317", "otlp export addrs");

bool TraceManager::Init() { return tracer_->Init(); }

void TraceManager::Stop() { tracer_->Stop(); };

TraceManager::TraceManager() {
  tracer_ = OpenTeleMetryTracer::New(
      FLAGS_trace_service_name, FLAGS_otlp_export_endpoint,
      dingofs::mds::GetGitCommitHash(), dingofs::mds::GetGitVersion());
}

SpanScopeSPtr TraceManager::StartSpan(const std::string& name) {
  auto scope = SpanScope::Create(shared_from_this(), name);

  scope->SetTraceSpan();
  return scope;
}

SpanScopeSPtr TraceManager::StartSpan(const std::string& name,
                                      const std::string& trace_id,
                                      const std::string& span_id) {
  auto scope = SpanScope::Create(shared_from_this(), name, trace_id, span_id);

  scope->SetTraceSpan();
  return scope;
}

SpanScopeSPtr TraceManager::StartChildSpan(const std::string& name,
                                           SpanScopeSptr parent) {
  auto scope = SpanScope::CreateChild(shared_from_this(), name, parent);
  scope->SetTraceSpan();
  return scope;
}

}  // namespace dingofs