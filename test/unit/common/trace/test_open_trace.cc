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

#include "common/opentrace/opentelemetry/tracer.h"

namespace dingofs {
DECLARE_bool(enable_trace);
DECLARE_string(otlp_export_endpoint);
namespace common {
namespace unit_test {
// ==========================================
// Simulate business loads
// ==========================================
void RunBusinessLogic() {
  volatile int sum = 0;
  for (int i = 0; i < 100; ++i) {
    sum += i;
  }
}

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
    tracer_ = OpenTeleMetryTracer::New("dingofs", FLAGS_otlp_export_endpoint);
    tracer_->Init();
  }

  void TearDown() override { tracer_->Stop(); }

  void RecursiveReal(int depth) {
    auto span = tracer_->MakeSpan("trace_operation");

    if (depth > 0) {
      RecursiveReal(depth - 1);
    }
  }

  void RecursiveBase(int depth) {
    RunBusinessLogic();
    if (depth > 0) {
      RecursiveBase(depth - 1);
    }
  }

 private:
  OpenTeleMetryTracerSPtr tracer_;
};

TEST_F(TraceManagerTest, AnalyzeOverhead) {
  // GTEST_SKIP() << "skip, run too long.";
  FLAGS_enable_trace = true;
  const int ITERATIONS = 100000;
  const int DEPTH = 0;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // BaseTime
  // ---------------------------------------------------------
  double t_base = MeasureAvgNanoseconds("1. Base (No Trace)", ITERATIONS,
                                        [this]() { RecursiveBase(DEPTH); });

  // ---------------------------------------------------------
  // Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("2. Real (Enable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  // double total_overhead = t_real - t_base;

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

  std::cout << "Base Logic Time:       " << t_base << " ns" << std::endl;
  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  // std::cout << "Total Overhead                 = " << std::setw(6)
  //           << total_overhead << " ns" << std::endl;

  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "================================================================"
      << std::endl;
}

TEST_F(TraceManagerTest, AnalyzeNestOverhead) {
  // GTEST_SKIP() << "skip, run too long.";
  FLAGS_enable_trace = true;
  const int ITERATIONS = 100000;
  const int DEPTH = 10;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  std::cout << "\n=== Nested Span Test (Depth = " << DEPTH << ") ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // scene 1: BaseTime
  // ---------------------------------------------------------
  double t_base = MeasureAvgNanoseconds("1. Base (No Trace)", ITERATIONS,
                                        [this]() { RecursiveBase(DEPTH); });

  // ---------------------------------------------------------
  // scene 2: Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("2. Real (Enable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  double total_overhead = t_real - t_base;

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

  std::cout << "Base Logic Time:       " << t_base << " ns" << std::endl;
  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout << "Total Overhead                 = " << std::setw(6)
            << total_overhead << " ns" << std::endl;

  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "================================================================"
      << std::endl;
}

TEST_F(TraceManagerTest, AnalyzeNoopOverhead) {
  FLAGS_enable_trace = false;
  const int ITERATIONS = 100000;
  const int DEPTH = 0;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // BaseTime
  // ---------------------------------------------------------
  double t_base = MeasureAvgNanoseconds("1. Base (No Trace)", ITERATIONS,
                                        [this]() { RecursiveBase(DEPTH); });

  // ---------------------------------------------------------
  // Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("2. Real (Disable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  // double total_overhead = t_real - t_base;

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

  std::cout << "Base Logic Time:       " << t_base << " ns" << std::endl;
  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  // std::cout << "Total Overhead                 = " << std::setw(6)
  //           << total_overhead << " ns" << std::endl;

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
  const int ITERATIONS = 100000;
  const int DEPTH = 10;

  std::cout << "\n=== Starting Trace Manager Performance Analysis ("
            << ITERATIONS << " iterations) ===\n"
            << std::endl;

  std::cout << "\n=== Nested Span Test (Depth = " << DEPTH << ") ===\n"
            << std::endl;

  // ---------------------------------------------------------
  // scene 1: BaseTime
  // ---------------------------------------------------------
  double t_base = MeasureAvgNanoseconds("1. Base (No Trace)", ITERATIONS,
                                        [this]() { RecursiveBase(DEPTH); });

  // ---------------------------------------------------------
  // scene 2: Real Trace
  // ---------------------------------------------------------
  double t_real = MeasureAvgNanoseconds("2. Real (Disable)", ITERATIONS,
                                        [this]() { RecursiveReal(DEPTH); });

  double total_overhead = t_real - t_base;

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

  std::cout << "Base Logic Time:       " << t_base << " ns" << std::endl;
  std::cout << "Real Logic Time:       " << t_real << " ns" << std::endl;
  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout << "Total Overhead                 = " << std::setw(6)
            << total_overhead << " ns" << std::endl;

  std::cout
      << "----------------------------------------------------------------"
      << std::endl;

  std::cout
      << "================================================================"
      << std::endl;
}

}  // namespace unit_test
}  // namespace common
}  // namespace dingofs
