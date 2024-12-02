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

#include "curvefs/src/aws/opentelemetry/OtelTracerProvider.h"

#include "curvefs/src/aws/opentelemetry/TraceAdapters.h"
#include "opentelemetry/trace/provider.h"

namespace curvefs {
namespace aws {
namespace opentelemetry {

using namespace smithy::components::tracing;

static const char* ALLOC_TAG = "OTEL_TRACER_PROVIDER";

std::shared_ptr<Tracer> OtelTracerProvider::GetTracer(
    Aws::String scope, const Aws::Map<Aws::String, Aws::String>& attributes) {
  (void)attributes;
  auto otel_tracer_provider =
      ::opentelemetry::trace::Provider::GetTracerProvider();
  auto otel_tracer = otel_tracer_provider->GetTracer(scope);
  return Aws::MakeShared<OtelTracerAdapter>(ALLOC_TAG, otel_tracer);
}

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs