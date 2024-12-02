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

#include "MeterAdapters.h"

#include <utility>

#include "aws/core/utils/logging/LogMacros.h"
#include "opentelemetry/context/runtime_context.h"
#include "opentelemetry/metrics/async_instruments.h"
#include "opentelemetry/metrics/observer_result.h"
#include "opentelemetry/metrics/sync_instruments.h"
#include "opentelemetry/nostd/variant.h"

namespace curvefs {
namespace aws {
namespace opentelemetry {

using namespace smithy::components::tracing;

static const char* ALLOC_TAG = "OTEL_METER_ADAPTER";

Aws::UniquePtr<MonotonicCounter> OtelMeterAdapter::CreateCounter(
    Aws::String name, Aws::String units, Aws::String description) const {
  auto counter = otelMeter_->CreateUInt64Counter(name, description, units);
  return Aws::MakeUnique<OtelCounterAdapter>(ALLOC_TAG, std::move(counter));
}

Aws::UniquePtr<GaugeHandle> OtelMeterAdapter::CreateGauge(
    Aws::String name,
    std::function<void(Aws::UniquePtr<AsyncMeasurement>)> callback,
    Aws::String units, Aws::String description) const {
  auto gauge = otelMeter_->CreateInt64ObservableGauge(name, description, units);
  GaugeHandleState gauge_handle_state{callback};
  auto callback_func = [](::opentelemetry::metrics::ObserverResult result,
                          void* state) -> void {
    if (state == nullptr) {
      AWS_LOG_ERROR(ALLOC_TAG, "refusing to process null observer result")
      return;
    }
    Aws::UniquePtr<AsyncMeasurement> measurement =
        Aws::MakeUnique<OtelObserverAdapter>(ALLOC_TAG, result);
    auto* handle_state = reinterpret_cast<GaugeHandleState*>(state);
    handle_state->callback(std::move(measurement));
  };
  gauge->AddCallback(callback_func, &gauge_handle_state);
  return Aws::MakeUnique<OtelGaugeAdapter>(ALLOC_TAG, std::move(gauge),
                                           callback_func);
}

Aws::UniquePtr<Histogram> OtelMeterAdapter::CreateHistogram(
    Aws::String name, Aws::String units, Aws::String description) const {
  auto histogram = otelMeter_->CreateDoubleHistogram(name, description, units);
  return Aws::MakeUnique<OtelHistogramAdapter>(ALLOC_TAG, std::move(histogram));
}

Aws::UniquePtr<UpDownCounter> OtelMeterAdapter::CreateUpDownCounter(
    Aws::String name, Aws::String units, Aws::String description) const {
  auto counter = otelMeter_->CreateInt64UpDownCounter(name, description, units);
  return Aws::MakeUnique<OtelUpDownCounterAdapter>(ALLOC_TAG,
                                                   std::move(counter));
}

OtelCounterAdapter::OtelCounterAdapter(
    ::opentelemetry::nostd::unique_ptr<
        ::opentelemetry::metrics::Counter<uint64_t>>
        counter)
    : otelCounter_(std::move(counter)) {}

void OtelCounterAdapter::add(long value,
                             Aws::Map<Aws::String, Aws::String> attributes) {
  otelCounter_->Add(value, attributes);
}

OtelUpDownCounterAdapter::OtelUpDownCounterAdapter(
    ::opentelemetry::nostd::unique_ptr<
        ::opentelemetry::metrics::UpDownCounter<int64_t>>
        counter)
    : otelUpDownCounter_(std::move(counter)) {}

void OtelUpDownCounterAdapter::add(
    long value, Aws::Map<Aws::String, Aws::String> attributes) {
  otelUpDownCounter_->Add(value, attributes);
}

OtelHistogramAdapter::OtelHistogramAdapter(
    ::opentelemetry::nostd::unique_ptr<
        ::opentelemetry::metrics::Histogram<double>>
        otel_histogram)
    : otelHistogram_(std::move(otel_histogram)) {}

void OtelHistogramAdapter::record(
    double value, Aws::Map<Aws::String, Aws::String> attributes) {
  otelHistogram_->Record(
      value, attributes,
      ::opentelemetry::context::RuntimeContext::GetCurrent());
}

OtelGaugeAdapter::OtelGaugeAdapter(
    ::opentelemetry::nostd::shared_ptr<
        ::opentelemetry::metrics::ObservableInstrument>
        otel_gauge,
    ::opentelemetry::metrics::ObservableCallbackPtr callback)
    : otelGauge_(std::move(otel_gauge)), otelCallback_(callback) {}

void OtelGaugeAdapter::Stop() {
  otelGauge_->RemoveCallback(otelCallback_, nullptr);
}

OtelObserverAdapter::OtelObserverAdapter(
    const ::opentelemetry::metrics::ObserverResult& result)
    : otelResult_(result) {}

void OtelObserverAdapter::Record(
    double value, const Aws::Map<Aws::String, Aws::String>& attributes) {
  auto result = ::opentelemetry::nostd::get<::opentelemetry::nostd::shared_ptr<
      ::opentelemetry::metrics::ObserverResultT<double>>>(otelResult_);
  result->Observe(value, attributes);
}

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs