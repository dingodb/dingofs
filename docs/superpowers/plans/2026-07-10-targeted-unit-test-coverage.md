# Targeted Unit Test Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in, per-binary GCC coverage reports to `test_mds`,
`test_common`, and `test_utils`.

**Architecture:** A test-only static library owns the `--coverage` flag,
counter lifecycle, `gcovr` invocation, reporting, and HTML serving. Each test
main passes a `CoverageConfig` that selects its source subtree and output
directory, so all coverage behavior is implemented once. `test_cache` remains
unchanged.

**Tech Stack:** C++17, CMake, Google Test, gflags, GCC `--coverage`, gcovr,
Python 3.

## Global Constraints

- Keep `test_cache` and its coverage implementation unchanged.
- Preserve normal test behavior when `--coverage` is absent.
- Target source filters exactly: `src/mds/`, `src/common/`, and `src/utils/`.
- Store reports under `build/coverage/test_mds/`, `test_common/`, and
  `test_utils/`.
- Coverage must work only with builds configured using GCC-compatible
  `--coverage -O0 -g -fprofile-update=atomic` compile flags and `--coverage`
  linker flags.
- Do not add CI publishing, thresholds, integration-test coverage, or an
  aggregate report.

---

## File Structure

| File | Responsibility |
| --- | --- |
| `test/unit/coverage/CMakeLists.txt` | Builds the reusable coverage library and its focused unit test. |
| `test/unit/coverage/coverage.h` | Declares `CoverageConfig`, `PrepareCoverageRun`, and `FinishCoverageRun`. |
| `test/unit/coverage/coverage.cc` | Implements flag handling, counter cleanup/flush, gcovr output, terminal summary, and local report server. |
| `test/unit/coverage/test_coverage.cc` | Tests report-command construction without executing gcovr. |
| `test/unit/CMakeLists.txt` | Adds the coverage helper before test targets that link it. |
| `test/unit/{mds,common,utils}/CMakeLists.txt` | Links each requested test binary to `test_coverage`. |
| `test/unit/{mds,common,utils}/main.cc` | Starts and finishes coverage around the existing test run. |
| `test/unit/{mds,common,utils}/README.md` | Documents instrumented build and per-target coverage invocation. |

### Task 1: Create the Reusable Coverage Helper

**Files:**
- Create: `test/unit/coverage/CMakeLists.txt`
- Create: `test/unit/coverage/coverage.h`
- Create: `test/unit/coverage/coverage.cc`
- Create: `test/unit/coverage/test_coverage.cc`
- Modify: `test/unit/CMakeLists.txt:31`

**Interfaces:**
- Produces:
  ```cpp
  namespace dingofs::unit_test {
  struct CoverageConfig {
    std::string target_name;
    std::string source_filter;
  };
  bool CoverageRequested();
  void PrepareCoverageRun();
  int FinishCoverageRun(const CoverageConfig& config, int test_rc);
  std::string BuildGcovrCommand(const CoverageConfig& config,
                                const std::string& repo_root,
                                const std::string& build_dir);
  }  // namespace dingofs::unit_test
  ```
- Consumes: gflags/glog already supplied through `${TEST_DEPS_WITHOUT_MAIN}`,
  GCC's weakly-linked `__gcov_dump`, `gcovr`, and `python3`.

- [ ] **Step 1: Write the failing command-construction test**

  Create `test/unit/coverage/test_coverage.cc`:

  ```cpp
  #include "test/unit/coverage/coverage.h"

  #include <gtest/gtest.h>

  namespace dingofs::unit_test {
  TEST(CoverageTest, BuildsTargetScopedGcovrCommand) {
    const CoverageConfig config{"test_mds", "src/mds/"};
    const std::string command =
        BuildGcovrCommand(config, "/repo", "/repo/build");

    EXPECT_NE(command.find("--filter '/repo/src/mds/'"), std::string::npos);
    EXPECT_NE(command.find("--html-details "
                           "'/repo/build/coverage/test_mds/index.html'"),
              std::string::npos);
    EXPECT_NE(command.find("--csv "
                           "'/repo/build/coverage/test_mds/summary.csv'"),
              std::string::npos);
    EXPECT_NE(command.find("' '/repo/build'"), std::string::npos);
  }
  }  // namespace dingofs::unit_test
  ```

- [ ] **Step 2: Add the failing target and run it**

  Create `test/unit/coverage/CMakeLists.txt`:

  ```cmake
  add_library(test_coverage STATIC coverage.cc)
  target_link_libraries(test_coverage ${TEST_DEPS_WITHOUT_MAIN})

  add_executable(test_coverage_helper test_coverage.cc)
  target_link_libraries(test_coverage_helper test_coverage ${TEST_DEPS_WITHOUT_MAIN})
  set_target_properties(test_coverage_helper PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${TEST_EXECUTABLE_OUTPUT_PATH})
  ```

  Add the helper before the dependent test directories in
  `test/unit/CMakeLists.txt`:

  ```cmake
  add_subdirectory(coverage)
  ```

  Run:

  ```bash
  cmake --build build --target test_coverage_helper -j 12
  ```

  Expected: compilation fails because `test/unit/coverage/coverage.h` does not
  exist.

- [ ] **Step 3: Implement the public API and helper behavior**

  Create `test/unit/coverage/coverage.h`:

  ```cpp
  #ifndef DINGOFS_TEST_UNIT_COVERAGE_COVERAGE_H_
  #define DINGOFS_TEST_UNIT_COVERAGE_COVERAGE_H_

  #include <string>

  namespace dingofs::unit_test {
  struct CoverageConfig {
    std::string target_name;
    std::string source_filter;
  };

  bool CoverageRequested();
  void PrepareCoverageRun();
  int FinishCoverageRun(const CoverageConfig& config, int test_rc);
  std::string BuildGcovrCommand(const CoverageConfig& config,
                                const std::string& repo_root,
                                const std::string& build_dir);
  }  // namespace dingofs::unit_test

  #endif  // DINGOFS_TEST_UNIT_COVERAGE_COVERAGE_H_
  ```

  In `coverage.cc`, move the generic mechanics from
  `test/unit/cache/main.cc` into the `dingofs::unit_test` namespace:

  ```cpp
  DEFINE_bool(coverage, false,
              "Generate and serve the selected test binary's line coverage.");

  bool CoverageRequested() { return FLAGS_coverage; }

  void PrepareCoverageRun() {
    if (CoverageRequested()) ClearStaleGcda(CoverageBuildDir());
  }

  int FinishCoverageRun(const CoverageConfig& config, int test_rc) {
    if (!CoverageRequested()) return test_rc;
    return RunCoverage(config, test_rc);
  }
  ```

  Preserve the cache implementation's behavior for:

  - resolving the executable with `/proc/self/exe`;
  - weakly declaring and checking `__gcov_dump`;
  - deleting stale `.gcda` files;
  - creating `build/coverage/<target_name>/`;
  - generating HTML details, text, and CSV using `gcovr`;
  - printing the CSV-based table with paths trimmed by
    `config.source_filter`;
  - serving the target-specific report directory on a loopback ephemeral
    Python HTTP server.

  Make command construction testable by implementing:

  ```cpp
  std::string BuildGcovrCommand(const CoverageConfig& config,
                                const std::string& repo_root,
                                const std::string& build_dir) {
    const std::string out_dir =
        build_dir + "/coverage/" + config.target_name;
    return "gcovr --root '" + repo_root + "' --filter '" + repo_root + "/" +
           config.source_filter +
           "' --gcov-executable gcov --gcov-ignore-errors=all"
           " --exclude-unreachable-branches --exclude-throw-branches"
           " --html-details '" + out_dir + "/index.html'"
           " --txt '" + out_dir + "/summary.txt'"
           " --csv '" + out_dir + "/summary.csv' '" + build_dir + "'";
  }
  ```

  `RunCoverage` must call this function and return `test_rc` on successful
  report generation when no report server can be started; otherwise, it must
  return nonzero on helper failures unless `test_rc` is already nonzero.

- [ ] **Step 4: Run the focused helper test**

  Run:

  ```bash
  cmake --build build --target test_coverage_helper -j 12
  ./build/bin/test/test_coverage_helper
  ```

  Expected: `CoverageTest.BuildsTargetScopedGcovrCommand` passes.

- [ ] **Step 5: Commit the helper**

  ```bash
  git add test/unit/coverage test/unit/CMakeLists.txt
  git commit -m "feat(test): add reusable coverage helper" \
    -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

### Task 2: Wire Coverage Into the Three Requested Test Binaries

**Files:**
- Modify: `test/unit/mds/CMakeLists.txt:24-67`
- Modify: `test/unit/common/CMakeLists.txt:19-39`
- Modify: `test/unit/utils/CMakeLists.txt:21-38`
- Modify: `test/unit/mds/main.cc:51-88`
- Modify: `test/unit/common/main.cc:21-36`
- Modify: `test/unit/utils/main.cc:21-50`

**Interfaces:**
- Consumes:
  ```cpp
  bool CoverageRequested();
  void PrepareCoverageRun();
  int FinishCoverageRun(const CoverageConfig& config, int test_rc);
  ```
- Produces: `--coverage` support for `test_mds`, `test_common`, and
  `test_utils`.

- [ ] **Step 1: Write the expected coverage configuration at each call site**

  Add the include and replace each final `return RUN_ALL_TESTS();` with the
  following pattern, preserving MDS's existing default filter:

  ```cpp
  #include "test/unit/coverage/coverage.h"

  // Immediately before RUN_ALL_TESTS().
  dingofs::unit_test::PrepareCoverageRun();
  const int rc = RUN_ALL_TESTS();
  return dingofs::unit_test::FinishCoverageRun(
      {"test_mds", "src/mds/"}, rc);
  ```

  Use these exact configurations in the other files:

  ```cpp
  {"test_common", "src/common/"}
  {"test_utils", "src/utils/"}
  ```

- [ ] **Step 2: Link all three binaries to the helper**

  Add `test_coverage` to each test executable's `target_link_libraries`:

  ```cmake
  target_link_libraries(test_mds
    test_coverage
    # Existing dependencies remain in their current order.
  )
  ```

  Apply the same addition to `test_common` and `test_utils`; do not modify
  `test_cache`.

- [ ] **Step 3: Build the three targets and confirm normal behavior**

  Run:

  ```bash
  cmake --build build --target test_mds test_common test_utils -j 12
  ./build/bin/test/test_common
  ./build/bin/test/test_utils
  ```

  Expected: each binary links and exits successfully without `--coverage`.

- [ ] **Step 4: Verify the uninstrumented coverage error**

  Run:

  ```bash
  ./build/bin/test/test_common --coverage
  ```

  Expected: the tests run, then the process exits nonzero with
  `coverage: binary not built with --coverage`; no misleading report is
  created.

- [ ] **Step 5: Commit target integration**

  ```bash
  git add test/unit/mds test/unit/common test/unit/utils
  git commit -m "feat(test): report coverage for mds common and utils" \
    -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

### Task 3: Document Instrumented Builds and Report Outputs

**Files:**
- Create: `test/unit/mds/README.md`
- Create: `test/unit/common/README.md`
- Create: `test/unit/utils/README.md`

**Interfaces:**
- Consumes: binary-specific source filters and output locations from Task 2.
- Produces: executable developer instructions for each coverage-enabled test.

- [ ] **Step 1: Add identical build prerequisites with target-specific commands**

  Use this CMake configuration in each README:

  ```bash
  cmake -S . -B build \
    -DBUILD_UNIT_TESTS=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DCMAKE_C_FLAGS="--coverage -O0 -g -fprofile-update=atomic" \
    -DCMAKE_CXX_FLAGS="--coverage -O0 -g -fprofile-update=atomic" \
    -DCMAKE_EXE_LINKER_FLAGS="--coverage" \
    -DCMAKE_SHARED_LINKER_FLAGS="--coverage"
  ```

  In `test/unit/mds/README.md`, append:

  ```bash
  cmake --build build --target test_mds -j 12
  ./build/bin/test/test_mds --coverage
  ```

  In the common and utils README files, substitute `test_common` and
  `test_utils` exactly.

- [ ] **Step 2: Document the generated report for each target**

  State the corresponding report directory:

  ```text
  build/coverage/test_mds/
  build/coverage/test_common/
  build/coverage/test_utils/
  ```

  State that the binary prints a per-file line-coverage table, serves
  `index.html` at `http://127.0.0.1:<random-port>/`, and requires `gcovr`,
  `python3`, and a `gcov` version matching the compiler. State that each run
  removes stale `.gcda` files before tests.

- [ ] **Step 3: Confirm documentation commands use the implemented interface**

  Run:

  ```bash
  git diff --check
  rg -n -- '--coverage|coverage/test_(mds|common|utils)' \
    test/unit/mds/README.md test/unit/common/README.md test/unit/utils/README.md
  ```

  Expected: each README shows its exact target name, `--coverage` invocation,
  and isolated report directory.

- [ ] **Step 4: Commit the documentation**

  ```bash
  git add test/unit/mds/README.md test/unit/common/README.md test/unit/utils/README.md
  git commit -m "docs(test): document targeted coverage reports" \
    -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
  ```

### Task 4: Validate an Instrumented Target End to End

**Files:**
- Modify: none

**Interfaces:**
- Consumes: the Task 1 helper and Task 2 `test_common --coverage` command.
- Produces: an actual `test_common` coverage report filtered to `src/common/`.

- [ ] **Step 1: Configure a separate instrumented build directory**

  Run:

  ```bash
  cmake -S . -B build \
    -DBUILD_UNIT_TESTS=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_FLAGS="--coverage -O0 -g -fprofile-update=atomic" \
    -DCMAKE_CXX_FLAGS="--coverage -O0 -g -fprofile-update=atomic" \
    -DCMAKE_EXE_LINKER_FLAGS="--coverage" \
    -DCMAKE_SHARED_LINKER_FLAGS="--coverage"
  cmake --build build --target test_common -j 12
  ```

  Expected: configuration and target build complete with GCC coverage
  instrumentation.

- [ ] **Step 2: Generate a real filtered report**

  Run:

  ```bash
  timeout 300s ./build/bin/test/test_common --coverage || test $? -eq 124
  test -f build/coverage/test_common/index.html
  test -f build/coverage/test_common/summary.csv
  rg -n '^src/common/' build/coverage/test_common/summary.csv
  ```

  Expected: the report exists and every CSV source path begins with
  `src/common/`. The timeout exit code is acceptable because the successful
  command replaces itself with the report server.

- [ ] **Step 3: Verify the final change set**

  Run:

  ```bash
  git diff --check
  git status --short
  ```

  Expected: no whitespace errors and only intentional pre-existing local
  changes remain.
