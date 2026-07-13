/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2026-07-10
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_COVERAGE_COVERAGE_H_
#define DINGOFS_TEST_UNIT_COVERAGE_COVERAGE_H_

#include <functional>
#include <string>
#include <vector>

namespace dingofs::unit_test {

// Identifies the test binary and the source tree slice its coverage report
// should be scoped to (e.g. "test_mds" / "src/mds/").
struct CoverageConfig {
  std::string target_name;
  std::string source_filter;
  // Paths (relative to the repo root, gcovr --exclude regexes) to omit from
  // the report -- e.g. benchmark mains or backends that require live cloud
  // infrastructure and so cannot be meaningfully unit tested.
  std::vector<std::string> excludes;
};

// True when the test binary was invoked with --coverage.
bool CoverageRequested();

// Absolute path to the repository source tree as configured by CMake
// (${CMAKE_SOURCE_DIR}). Unlike deriving the source root from
// /proc/self/exe, this stays correct even when the build directory is a
// symlink that resolves outside the checkout.
std::string CoverageSourceDir();

// Runs run_tests() (typically `[] { return RUN_ALL_TESTS(); }`) and, when
// CoverageRequested() is true, produces the gcovr report for config
// afterwards. When CoverageRequested() is false this simply returns
// run_tests()'s result.
//
// GCC 13 removed the in-process __gcov_dump()/__gcov_flush()/__gcov_reset()
// API; coverage counters are now only flushed to .gcda on normal process
// exit. So run_tests() executes in a forked child that exits normally
// (flushing its counters), and the report is generated in the parent once
// the child's .gcda files are on disk.
int RunTestsWithCoverage(const CoverageConfig& config,
                         const std::function<int()>& run_tests);

// Builds the gcovr invocation used to produce config's coverage report.
// Exposed for unit testing.
std::string BuildGcovrCommand(const CoverageConfig& config,
                              const std::string& repo_root,
                              const std::string& build_dir);

}  // namespace dingofs::unit_test

#endif  // DINGOFS_TEST_UNIT_COVERAGE_COVERAGE_H_
