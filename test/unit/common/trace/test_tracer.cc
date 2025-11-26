// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <butil/time.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "common/trace/log_trace_exporter.h"
#include "common/trace/tracer.h"
#include "common/trace/utils.h"

namespace dingofs {
namespace common {
namespace unit_test {

const size_t kTestIterations = 100000;

class TracerPerformanceTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  void PrintPerformanceResults(const std::string& test_name, size_t iterations,
                               butil::Timer& timer) {
    double avg_time_ns = timer.n_elapsed(0.0) / iterations;

    std::cout << "\nPerformance Test Results - " << test_name << "\n";
    std::cout << "========================================\n";
    std::cout << "Test iterations:      " << iterations << "\n";
    std::cout << "Total time:          " << timer.m_elapsed(0.0) << " ms\n";
    std::cout << "Average time:        " << avg_time_ns << " ns per call\n";
    std::cout << "Throughput:     " << (iterations / timer.s_elapsed(0.0))
              << " calls/second\n";
    std::cout << "----------------------------------------\n";
  }
};

TEST_F(TracerPerformanceTest, BenchStartSpan) {
  auto tracer = std::make_unique<Tracer>(
      std::make_unique<LogTraceExporter>("test-span", "/tmp"));

  butil::Timer timer;
  timer.start();
  for (size_t i = 0; i < kTestIterations; ++i) {
    auto span = tracer->StartSpan("test_module", "test_span");
  }

  timer.stop();

  PrintPerformanceResults("StartSpan", kTestIterations, timer);
}

TEST_F(TracerPerformanceTest, BenchStartSpanWithParent) {
  auto tracer = std::make_unique<Tracer>(
      std::make_unique<LogTraceExporter>("test-parent", "/tmp"));
  auto parent_span = tracer->StartSpan("test_module", "parent_span");

  butil::Timer timer;
  timer.start();

  for (size_t i = 0; i < kTestIterations; ++i) {
    auto span =
        tracer->StartSpanWithParent("test_module", "test_span", *parent_span);
  }

  timer.stop();

  PrintPerformanceResults("StartSpanWithParent", kTestIterations, timer);
}

TEST_F(TracerPerformanceTest, BenchStartSpanWithContext) {
  auto tracer = std::make_unique<Tracer>(
      std::make_unique<LogTraceExporter>("test-context", "/tmp"));
  auto parent_span = tracer->StartSpan("test_module", "parent_span");
  auto parent_ctx = parent_span->GetContext();

  butil::Timer timer;
  timer.start();

  for (size_t i = 0; i < kTestIterations; ++i) {
    auto span =
        tracer->StartSpanWithContext("test_module", "test_span", parent_ctx);
  }

  timer.stop();

  PrintPerformanceResults("StartSpanWithContext", kTestIterations, timer);
}

TEST_F(TracerPerformanceTest, BenchMixedSpanCreation) {
  auto tracer = std::make_unique<Tracer>(
      std::make_unique<LogTraceExporter>("test-mixed", "/tmp"));
  auto parent_span = tracer->StartSpan("test_module", "parent_span");
  auto parent_ctx = parent_span->GetContext();

  size_t start_span_count_ = 0;
  size_t with_parent_count_ = 0;
  size_t with_context_count_ = 0;

  constexpr int ratios[] = {2, 4, 4};
  std::discrete_distribution<> dist(std::begin(ratios), std::end(ratios));
  std::mt19937 gen(std::random_device{}());

  butil::Timer timer;
  timer.start();

  for (size_t i = 0; i < kTestIterations; ++i) {
    switch (dist(gen)) {
      case 0:
        tracer->StartSpan("test_module", "test_span");
        ++start_span_count_;
        break;
      case 1:
        tracer->StartSpanWithParent("test_module", "test_span", *parent_span);
        ++with_parent_count_;
        break;
      case 2:
        tracer->StartSpanWithContext("test_module", "test_span", parent_ctx);
        ++with_context_count_;
        break;
      default:
        CHECK(false) << "Unexpected distribution value";
    }
  }

  timer.stop();
  std::cout << "StartSpan:           " << start_span_count_ << " calls\n";
  std::cout << "StartSpanWithParent: " << with_parent_count_ << " calls\n";
  std::cout << "StartSpanWithContext:" << with_context_count_ << " calls\n";

  PrintPerformanceResults("Mixed Creation", kTestIterations, timer);
}

static void PerformanceTest(size_t id_length, size_t iterations) {
  butil::Timer timer;
  timer.start();

  for (size_t i = 0; i < iterations; ++i) {
    GenerateId(id_length);
  }

  timer.stop();

  // Calculate and print results
  double avg_time_ns = timer.n_elapsed(0.0) / iterations;

  std::cout << "Performance Test Results:\n";
  std::cout << "========================\n";
  std::cout << "ID Length:      " << id_length << " bytes (output will be "
            << id_length * 2 << " chars)\n";
  std::cout << "Iterations:     " << iterations << "\n";
  std::cout << "Total time:     " << timer.m_elapsed(0.0) << " ms\n";
  std::cout << "Average time:   " << avg_time_ns << " ns per call\n";
  std::cout << "Throughput:     " << (iterations / timer.s_elapsed(0.0))
            << " calls/second\n";
  std::cout << "Sample output:  " << GenerateId(id_length) << "\n";
}

TEST(GenerateIdTest, Benchmark) {
  // Test different ID lengths
  const size_t test_lengths[] = {kTraceIdLength, kSpanIdLength};
  const size_t iterations = 100000;

  for (auto length : test_lengths) {
    PerformanceTest(length, iterations);
    std::cout << "\n";
  }
}

}  // namespace unit_test
}  // namespace common
}  // namespace dingofs
