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

#include "curvefs/src/aws/opentelemetry/OtelTelemetryProvider.h"

#include <memory>

#include "curvefs/src/aws/opentelemetry/OtelMeterProvider.h"
#include "curvefs/src/aws/opentelemetry/OtelTracerProvider.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"

namespace curvefs {
namespace aws {
namespace opentelemetry {
using namespace smithy::components::tracing;

static const char* ALLOC_TAG = "OTEL_TELEMETRY_PROVIDER";

Aws::UniquePtr<TelemetryProvider> OtelTelemetryProvider::CreateOtelProvider(
    std::unique_ptr<::opentelemetry::sdk::trace::SpanExporter> span_exporter,
    std::unique_ptr<::opentelemetry::sdk::metrics::PushMetricExporter>
        push_metric_exporter) {
  auto tracer_provider = Aws::MakeUnique<OtelTracerProvider>(ALLOC_TAG);
  auto meter_provider = Aws::MakeUnique<OtelMeterProvider>(ALLOC_TAG);

  return Aws::MakeUnique<TelemetryProvider>(
      ALLOC_TAG, std::move(tracer_provider), std::move(meter_provider),
      [&]() -> void {
        // Init Tracing
        auto trace_processor =
            ::opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(
                std::move(span_exporter));
        std::shared_ptr<::opentelemetry::trace::TracerProvider>
            otel_tracer_provider =
                ::opentelemetry::sdk::trace::TracerProviderFactory::Create(
                    std::move(trace_processor));
        ::opentelemetry::trace::Provider::SetTracerProvider(
            otel_tracer_provider);

        // Init Metrics
        ::opentelemetry::sdk::metrics::PeriodicExportingMetricReaderOptions
            options;
        options.export_interval_millis = std::chrono::milliseconds(1000);
        options.export_timeout_millis = std::chrono::milliseconds(500);
        std::unique_ptr<::opentelemetry::sdk::metrics::MetricReader> reader{
            new ::opentelemetry::sdk::metrics::PeriodicExportingMetricReader(
                std::move(push_metric_exporter), options)};

        auto otel_meter_provider =
            std::shared_ptr<::opentelemetry::metrics::MeterProvider>(
                new ::opentelemetry::sdk::metrics::MeterProvider());
        auto p = std::static_pointer_cast<
            ::opentelemetry::sdk::metrics::MeterProvider>(otel_meter_provider);
        p->AddMetricReader(std::move(reader));
        ::opentelemetry::metrics::Provider::SetMeterProvider(
            otel_meter_provider);
      },
      []() -> void {
        // Clean up tracing
        std::shared_ptr<::opentelemetry::trace::TracerProvider> empty_tracer;
        ::opentelemetry::trace::Provider::SetTracerProvider(empty_tracer);
        // Clean up metrics
        std::shared_ptr<::opentelemetry::metrics::MeterProvider> empty_meter;
        ::opentelemetry::metrics::Provider::SetMeterProvider(empty_meter);
      });
}

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs