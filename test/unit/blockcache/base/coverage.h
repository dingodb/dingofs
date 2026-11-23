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
 * Created Date: 2026-10-06
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_COVERAGE_H_
#define DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_COVERAGE_H_

#include <functional>
#include <string>
#include <vector>

namespace dingofs {
namespace blockcache {

// Identifies the test binary and the source tree slice its coverage report
// should be scoped to (e.g. "test_blockcache" / "src/blockcache/").
struct CoverageConfig {
  std::string target_name;
  std::string source_filter;
  // Paths (relative to the repo root, gcovr --exclude regexes) to omit from
  // the report -- e.g. benchmark mains that cannot be meaningfully unit
  // tested.
  std::vector<std::string> excludes;
  // Where the tests live, relative to the repo root (e.g.
  // "test/unit/blockcache/"). When set, a gtest filter narrows the report to
  // the sources of the tests it picks: <test_dir><dir>/test_<name>.cc tests
  // <source_filter><dir>/<name>.{h,cc}.
  std::string test_dir;
};

// True when the test binary was invoked with --coverage, and in the child
// that RunTestsWithCoverage re-executes to run the tests.
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
// exit. So run_tests() executes in a child process that exits normally
// (flushing its counters), and the report is generated in the parent once
// the child's .gcda files are on disk. The child is created by re-executing
// this same binary (with the argc/argv given) rather than by a bare fork(): a
// forked child of a multi-threaded process keeps only the forking thread, so
// it could block forever on a lock another thread held.
//
// Pass argc/argv as main() got them, before gtest and gflags take their flags
// out: they are the child's command line, so it runs the tests the parent was
// asked for.
int RunTestsWithCoverage(const CoverageConfig& config, int argc, char** argv,
                         const std::function<int()>& run_tests);

// A regex, relative to config.source_filter, of the sources tested by the
// tests that the gtest filter picks; "" (the whole source tree) when
// config.test_dir is unset, the filter is "*", or no picked test follows
// test_dir's naming. Exposed for unit testing.
std::string TestedSources(const CoverageConfig& config,
                          const std::string& repo_root,
                          const std::string& filter);

// Builds the gcovr invocation used to produce config's coverage report,
// narrowed to the sources matching the regex `sources` (see TestedSources)
// unless it is empty. Exposed for unit testing.
std::string BuildGcovrCommand(const CoverageConfig& config,
                              const std::string& repo_root,
                              const std::string& build_dir,
                              const std::string& sources);

// Prints the HTML report's path and returns test_rc unchanged. Exposed for
// unit testing.
int FinishCoverageReport(const std::string& html, int test_rc);

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_COVERAGE_H_
