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

#ifndef SRC_AWS_S3_OPENTELEMETRY_METER_ADAPTER_H_
#define SRC_AWS_S3_OPENTELEMETRY_METER_ADAPTER_H_

#include <opentelemetry/metrics/async_instruments.h>
#include <opentelemetry/metrics/meter.h>
#include <opentelemetry/metrics/observer_result.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <smithy/tracing/Meter.h>

#include <utility>

namespace curvefs {
namespace aws {
namespace opentelemetry {

using namespace smithy::components::tracing;

/**
 * A Open Telemetry Implementation of Meter.
 */
class OtelMeterAdapter final : public Meter {
 public:
  explicit OtelMeterAdapter(
      ::opentelemetry::nostd::shared_ptr<::opentelemetry::metrics::Meter> meter)
      : otelMeter_(std::move(meter)) {}

  Aws::UniquePtr<GaugeHandle> CreateGauge(
      Aws::String name,
      std::function<void(Aws::UniquePtr<AsyncMeasurement>)> callback,
      Aws::String units, Aws::String description) const override;

  Aws::UniquePtr<UpDownCounter> CreateUpDownCounter(
      Aws::String name, Aws::String units,
      Aws::String description) const override;

  Aws::UniquePtr<MonotonicCounter> CreateCounter(
      Aws::String name, Aws::String units,
      Aws::String description) const override;

  Aws::UniquePtr<Histogram> CreateHistogram(
      Aws::String name, Aws::String units,
      Aws::String description) const override;

 private:
  ::opentelemetry::nostd::shared_ptr<::opentelemetry::metrics::Meter>
      otelMeter_;
};

/**
 * A Open Telemetry Implementation of MonotonicCounter.
 */
class OtelCounterAdapter final : public MonotonicCounter {
 public:
  explicit OtelCounterAdapter(::opentelemetry::nostd::unique_ptr<
                              ::opentelemetry::metrics::Counter<uint64_t>>
                                  counter);

  void add(long value, Aws::Map<Aws::String, Aws::String> attributes) override;

 private:
  ::opentelemetry::nostd::unique_ptr<
      ::opentelemetry::metrics::Counter<uint64_t>>
      otelCounter_;
};

/**
 * A Open Telemetry Implementation of UpDownCounter.
 */
class OtelUpDownCounterAdapter final : public UpDownCounter {
 public:
  explicit OtelUpDownCounterAdapter(
      ::opentelemetry::nostd::unique_ptr<
          ::opentelemetry::metrics::UpDownCounter<int64_t>>
          counter);

  void add(long value, Aws::Map<Aws::String, Aws::String> attributes) override;

 private:
  ::opentelemetry::nostd::unique_ptr<
      ::opentelemetry::metrics::UpDownCounter<int64_t>>
      otelUpDownCounter_;
};

/**
 * A Open Telemetry Implementation of Histogram.
 */
class OtelHistogramAdapter final : public Histogram {
 public:
  explicit OtelHistogramAdapter(::opentelemetry::nostd::unique_ptr<
                                ::opentelemetry::metrics::Histogram<double>>
                                    otel_histogram);

  void record(double value,
              Aws::Map<Aws::String, Aws::String> attributes) override;

 private:
  ::opentelemetry::nostd::unique_ptr<
      ::opentelemetry::metrics::Histogram<double>>
      otelHistogram_;
};

/**
 * A struct type for the C function pointer interface to pass state.
 */
struct GaugeHandleState {
  std::function<void(Aws::UniquePtr<AsyncMeasurement>)> callback;
};

/**
 * A Open Telemetry Implementation of GaugeHandle.
 */
class OtelGaugeAdapter final : public GaugeHandle {
 public:
  explicit OtelGaugeAdapter(
      ::opentelemetry::nostd::shared_ptr<
          ::opentelemetry::metrics::ObservableInstrument>
          otel_gauge,
      ::opentelemetry::metrics::ObservableCallbackPtr callback);

  void Stop() override;

 private:
  ::opentelemetry::nostd::shared_ptr<
      ::opentelemetry::metrics::ObservableInstrument>
      otelGauge_;
  ::opentelemetry::metrics::ObservableCallbackPtr otelCallback_;
};

/**
 * A Open Telemetry Implementation of AsyncMeasurement.
 */
class OtelObserverAdapter final : public AsyncMeasurement {
 public:
  explicit OtelObserverAdapter(
      const ::opentelemetry::metrics::ObserverResult& otel_result);

  void Record(double value,
              const Aws::Map<Aws::String, Aws::String>& attributes) override;

 private:
  const ::opentelemetry::metrics::ObserverResult& otelResult_;
};

}  // namespace opentelemetry
}  // namespace aws
}  // namespace curvefs
#endif  // SRC_AWS_S3_OPENTELEMETRY_METER_ADAPTER_H_