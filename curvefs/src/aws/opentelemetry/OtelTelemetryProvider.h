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

#ifndef SRC_AWS_S3_OPENTELEMETRY_OTEL_TELEMETRY_PROVIDER_H_
#define SRC_AWS_S3_OPENTELEMETRY_OTEL_TELEMETRY_PROVIDER_H_

#include <opentelemetry/sdk/metrics/push_metric_exporter.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <smithy/tracing/TelemetryProvider.h>

namespace curvefs {
namespace aws {
namespace opentelemetry {

using smithy::components::tracing::TelemetryProvider;

/**
 * A Open Telemetry Implementation of TelemetryProvider.
 */
class OtelTelemetryProvider final : public TelemetryProvider {
 public:
  static Aws::UniquePtr<TelemetryProvider> CreateOtelProvider(
      std::unique_ptr<::opentelemetry::sdk::trace::SpanExporter> span_exporter,
      std::unique_ptr<::opentelemetry::sdk::metrics::PushMetricExporter>
          push_metric_exporter);
};

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs

#endif  // SRC_AWS_S3_OPENTELEMETRY_OTEL_TELEMETRY_PROVIDER_H_