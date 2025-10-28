
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

#include "common/trace/utils.h"

namespace dingofs {

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

}  // namespace dingofs

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);

  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  return RUN_ALL_TESTS();
}