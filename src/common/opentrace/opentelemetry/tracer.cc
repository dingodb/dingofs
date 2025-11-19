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

#include "common/opentrace/opentelemetry/tracer.h"

#include <memory>

#include "common/opentrace/opentelemetry/noop_span.h"
#include "common/opentrace/opentelemetry/otlp_span.h"

namespace dingofs {

DEFINE_bool(enable_trace, false, "Whether to enable trace");

bool OpenTeleMetryTracer::Init() {
  auto resource_attributes = opentelemetry::sdk::resource::ResourceAttributes{
      {"service.name", service_name_}, {"service.commit", commit_hash_}};

  auto resource = resource::Resource::Create(resource_attributes);

  otlp::OtlpGrpcExporterOptions opts;
  opts.endpoint = otlp_export_endpoint_;

  auto exporter = std::unique_ptr<trace_sdk::SpanExporter>(
      new otlp::OtlpGrpcExporter(opts));

  // https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk.md#batching-processor

  // Export is triggered when any of the following conditions is met:
  // The number of spans in the queue reaches maxExportBatchSize
  // It has surpassed schedule_delay_millis since the last export
  trace_sdk::BatchSpanProcessorOptions options;

  // the maximum queue size. After the size is reached spans are dropped.
  options.max_queue_size = 8192;
  options.schedule_delay_millis = std::chrono::milliseconds(5000);
  options.max_export_batch_size = 1024;

  //  Create a BatchSpanProcessor
  auto processor = std::unique_ptr<trace_sdk::BatchSpanProcessor>(
      new trace_sdk::BatchSpanProcessor(std::move(exporter), options));

  // Create a TracerProvider, and add the processor and resource
  auto provider = nostd::shared_ptr<trace::TracerProvider>(
      new trace_sdk::TracerProvider(std::move(processor), resource));

  // Set the global tracer provider
  trace::Provider::SetTracerProvider(provider);

  tracer_ = provider->GetTracer(service_name_, version_);
  return true;
}

void OpenTeleMetryTracer::Stop() {
  // To prevent cancelling ongoing exports.
  nostd::shared_ptr<trace::TracerProvider> provider =
      trace::Provider::GetTracerProvider();

  if (provider) {
    if (trace_sdk::TracerProvider* d =
            dynamic_cast<trace_sdk::TracerProvider*>(provider.get());
        d) {
      d->ForceFlush();
    }
  }
  nostd::shared_ptr<trace::TracerProvider> noop(
      new trace::NoopTracerProvider());
  trace::Provider::SetTracerProvider(noop);
};

std::shared_ptr<Span> OpenTeleMetryTracer::MakeSpan(const std::string& name) {
  if (!FLAGS_enable_trace) {
    return std::make_shared<NoopSpan>();
  }
  return std::make_shared<OtlpSpan>(tracer_->StartSpan(name));
}

std::shared_ptr<Span> OpenTeleMetryTracer::MakeSpan(
    const std::string& name, const SpanContext& span_context) {
  if (!FLAGS_enable_trace) {
    return std::make_shared<NoopSpan>();
  }
  trace::StartSpanOptions options;
  options.parent = span_context;
  return std::make_shared<OtlpSpan>(tracer_->StartSpan(name, options));
}

std::shared_ptr<Span> OpenTeleMetryTracer::MakeSpan(
    const std::string& name, const std::string& trace_id,
    const std::string& span_id) {
  if (!FLAGS_enable_trace) {
    return std::make_shared<NoopSpan>();
  }

  auto otel_trace_id =
      trace::propagation::HttpTraceContext::TraceIdFromHex(trace_id);
  auto otel_span_id =
      trace::propagation::HttpTraceContext::SpanIdFromHex(span_id);

  auto span_context =
      trace::SpanContext(otel_trace_id, otel_span_id,
                         opentelemetry::trace::TraceFlags{
                             opentelemetry::trace::TraceFlags::kIsSampled},
                         true  // is_remote
      );

  // Create a server span that is a child of the extracted context
  trace::StartSpanOptions options;
  options.parent = span_context;  // Set the parent context
  return std::make_shared<OtlpSpan>(tracer_->StartSpan(name, options));
}

}  // namespace dingofs