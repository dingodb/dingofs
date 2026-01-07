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
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "common/trace/trace_manager.h"
#include "utils/time.h"

namespace dingofs {
DECLARE_bool(enable_trace);
DECLARE_string(otlp_export_endpoint);
namespace common {
namespace unit_test {

template <typename Func>
double MeasureAvgNanoseconds(const std::string& label, int iterations,
                             Func func) {
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; ++i) {
    func();
  }
  auto end = std::chrono::high_resolution_clock::now();

  long long total_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  double avg = (double)total_ns / iterations;

  std::cout << "[RAW] " << std::left << std::setw(25) << label << ": " << avg
            << " ns/op" << std::endl;
  return avg;
}

class TraceManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tracer_manager_ = TraceManager::New();
    tracer_manager_->Init();
  }

  void TearDown() override { tracer_manager_->Stop(); }

  void RecursiveReal(int depth) {
    auto span = tracer_manager_->StartSpan("trace_operation");

    if (depth > 0) {
      RecursiveReal(depth - 1);
    }
  }

 private:
  TraceManagerSPtr tracer_manager_;
};

TEST_F(TraceManagerTest, AnalyzeOverhead) {
  // GTEST_SKIP() << "skip, run too long.";
  FLAGS_enable_trace = true;
  const int ITERATIONS = 100;
  const int DEPTH = 0;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("1. Real (Enable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  std::cout
      << "\n================================================================"
      << std::endl;
  std::cout
      << "                      FINAL REPORT                              "
      << std::endl;
  std::cout
      << "================================================================"
      << std::endl;

  std::cout << std::fixed << std::setprecision(2);

  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "----------------------------------------------------------------"
      << std::endl;
}

TEST_F(TraceManagerTest, AnalyzeNestOverhead) {
  // GTEST_SKIP() << "skip, run too long.";
  FLAGS_enable_trace = true;
  const int ITERATIONS = 100;
  const int DEPTH = 10;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  std::cout << "\n=== Nested Span Test (Depth = " << DEPTH << ") ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // scene : Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("2. Real (Enable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  std::cout
      << "\n================================================================"
      << std::endl;
  std::cout
      << "                      FINAL REPORT                              "
      << std::endl;
  std::cout
      << "================================================================"
      << std::endl;

  std::cout << std::fixed << std::setprecision(2);

  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "================================================================"
      << std::endl;
}

TEST_F(TraceManagerTest, AnalyzeNoopOverhead) {
  FLAGS_enable_trace = false;
  const int ITERATIONS = 100;
  const int DEPTH = 0;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("1. Real (Disable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  std::cout
      << "\n================================================================"
      << std::endl;
  std::cout
      << "                      FINAL REPORT                              "
      << std::endl;
  std::cout
      << "================================================================"
      << std::endl;

  std::cout << std::fixed << std::setprecision(2);

  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "================================================================"
      << std::endl;
}

TEST_F(TraceManagerTest, AnalyzeNoopNestOverhead) {
  // GTEST_SKIP() << "skip, run too long.";
  FLAGS_enable_trace = false;
  const int ITERATIONS = 100;
  const int DEPTH = 10;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  std::cout << "\n=== Nested Span Test (Depth = " << DEPTH << ") ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // scene: Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("1. Real (Disable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  std::cout
      << "\n================================================================"
      << std::endl;
  std::cout
      << "                      FINAL REPORT                              "
      << std::endl;
  std::cout
      << "================================================================"
      << std::endl;

  std::cout << std::fixed << std::setprecision(2);

  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "================================================================"
      << std::endl;
}

static void PerformanceTest(size_t iterations) {
  butil::Timer timer;
  timer.start();

  for (size_t i = 0; i < iterations; ++i) {
    std::to_string(utils::TimestampNs());
  }

  timer.stop();

  // Calculate and print results
  double avg_time_ns = timer.n_elapsed(0.0) / iterations;

  std::cout << "Performance Test Results:\n";
  std::cout << "========================\n";

  std::cout << "Iterations:     " << iterations << "\n";
  std::cout << "Total time:     " << timer.m_elapsed(0.0) << " ms\n";
  std::cout << "Average time:   " << avg_time_ns << " ns per call\n";
  std::cout << "Throughput:     " << (iterations / timer.s_elapsed(0.0))
            << " calls/second\n";
  std::cout << "Sample output:  " << std::to_string(utils::TimestampNs())
            << "\n";
}

TEST(GenerateSessionIDTest, Benchmark) {
  // Test different ID lengths
  const size_t iterations = 100000;

  PerformanceTest(iterations);
}

}  // namespace unit_test
}  // namespace common
}  // namespace dingofs
