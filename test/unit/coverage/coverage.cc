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

#include "test/unit/coverage/coverage.h"

#ifndef DINGOFS_SOURCE_DIR
#error "DINGOFS_SOURCE_DIR must be defined by the test_coverage CMake target"
#endif

#include <gflags/gflags.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

extern char** environ;

DEFINE_bool(coverage, false,
            "Generate the selected test binary's line coverage report.");

namespace dingofs::unit_test {

// Prints the HTML report path and returns test_rc unchanged. Not declared in
// coverage.h (internal to the report-generation flow); forward-declared here
// so RunCoverage() can call it, and in test_coverage.cc to exercise it in
// isolation.
int FinishCoverageReport(const std::string& html, int test_rc);

namespace {

std::string Dirname(const std::string& path) {
  auto pos = path.find_last_of('/');
  return pos == std::string::npos ? "." : path.substr(0, pos);
}

std::string SelfPath() {
  char buf[4096];
  ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
  if (n <= 0) return "";
  return std::string(buf, n);
}

// Binary lives at <build_dir>/bin/test/<target_name>.
std::string CoverageBuildDir() {
  const std::string exe = SelfPath();
  return exe.empty() ? "" : Dirname(Dirname(Dirname(exe)));
}

// gcov merges counters into existing .gcda files, so old runs would otherwise
// inflate the report. Drop them before this run so coverage reflects it alone.
void ClearStaleGcda(const std::string& build_dir) {
  if (build_dir.empty()) return;
  std::error_code ec;
  for (std::filesystem::recursive_directory_iterator it(build_dir, ec), end;
       it != end; it.increment(ec)) {
    if (it->path().extension() == ".gcda") {
      std::filesystem::remove(it->path(), ec);
    }
  }
}

struct FileCov {
  std::string name;
  long total = 0;
  long covered = 0;
};

int Pct(long covered, long total) {
  return total == 0 ? 100
                    : static_cast<int>(std::lround(100.0 * covered / total));
}

const char* PctColor(int pct, bool tty) {
  if (!tty) return "";
  if (pct >= 90) return "\033[32m";  // green
  if (pct >= 75) return "\033[33m";  // yellow
  return "\033[31m";                 // red
}

// Parse the gcovr CSV: filename,line_total,line_covered,line_percent,...
std::vector<FileCov> ParseCsv(const std::string& csv_path) {
  std::vector<FileCov> rows;
  std::ifstream in(csv_path);
  std::string line;
  bool header = true;
  while (std::getline(in, line)) {
    if (header) {
      header = false;
      continue;
    }
    if (line.empty()) continue;
    std::vector<std::string> cols;
    std::stringstream ss(line);
    std::string cell;
    while (std::getline(ss, cell, ',')) cols.push_back(cell);
    if (cols.size() < 3) continue;
    rows.push_back({cols[0], std::strtol(cols[1].c_str(), nullptr, 10),
                    std::strtol(cols[2].c_str(), nullptr, 10)});
  }
  return rows;
}

void PrintTable(const CoverageConfig& config,
                const std::vector<FileCov>& rows) {
  const bool tty = ::isatty(STDOUT_FILENO) != 0;
  const char* reset = tty ? "\033[0m" : "";
  const std::string& prefix = config.source_filter;

  auto display_name = [&](const std::string& n) {
    return n.rfind(prefix, 0) == 0 ? n.substr(prefix.size()) : n;
  };

  int namew = 4;  // "File"
  long sum_total = 0, sum_covered = 0;
  for (const auto& r : rows) {
    namew = std::max<int>(namew, display_name(r.name).size());
    sum_total += r.total;
    sum_covered += r.covered;
  }

  auto row = [&](const std::string& name, long total, long covered, int pct) {
    std::cout << std::left << std::setw(namew) << name << ' ' << std::right
              << std::setw(8) << total << ' ' << std::setw(8) << covered << "  "
              << PctColor(pct, tty) << std::setw(4) << pct << '%' << reset
              << '\n';
  };

  const std::string bar(namew + 26, '=');
  const std::string dash(namew + 26, '-');
  std::cout << '\n'
            << bar << "\n  " << prefix << " line coverage\n"
            << dash << '\n';
  std::cout << std::left << std::setw(namew) << "File" << ' ' << std::right
            << std::setw(8) << "Lines" << ' ' << std::setw(8) << "Covered"
            << "  " << std::setw(5) << "Cover" << '\n'
            << dash << '\n';
  for (const auto& r : rows) {
    row(display_name(r.name), r.total, r.covered, Pct(r.covered, r.total));
  }
  std::cout << dash << '\n';
  row("TOTAL", sum_total, sum_covered, Pct(sum_covered, sum_total));
  std::cout << bar << '\n';
}

// Run gcovr, print the summary table, then print the HTML report's path.
// Returns test_rc so the caller can propagate the underlying test result.
// build_dir's .gcda files are expected to already be on disk (the caller
// waited for the test process to exit normally).
int RunCoverage(const CoverageConfig& config, const std::string& build_dir,
                int test_rc) {
  // gcovr's --filter matches against realpath(candidate_file), resolving any
  // symlinks in the source tree, but never resolves the --filter pattern
  // itself. So repo_root must already be symlink-free, or filters built from
  // it silently match nothing whenever the checkout is reached through a
  // symlink (e.g. /home/user/proj -> /mnt/disk/proj).
  std::error_code ec;
  std::string repo_root =
      std::filesystem::canonical(CoverageSourceDir(), ec).string();
  if (ec) repo_root = CoverageSourceDir();

  const std::string out_dir = build_dir + "/coverage/" + config.target_name;
  const std::string html = out_dir + "/index.html";
  const std::string csv = out_dir + "/summary.csv";

  std::filesystem::create_directories(out_dir, ec);
  if (ec) {
    std::cerr << "coverage: cannot create " << out_dir << '\n';
    return test_rc != 0 ? test_rc : 1;
  }

  const std::string cmd = BuildGcovrCommand(config, repo_root, build_dir);

  std::cout << "\n";
  if (std::system(cmd.c_str()) != 0) {  // NOLINT(cert-env33-c)
    std::cerr << "coverage: gcovr failed. Did you build with --coverage and "
                 "run the tests first?\n";
    return test_rc != 0 ? test_rc : 1;
  }

  PrintTable(config, ParseCsv(csv));

  return FinishCoverageReport(html, test_rc);
}

}  // namespace

namespace {
// Marks the re-exec'd child so it runs tests directly instead of spawning
// yet another child (which would otherwise recurse forever).
constexpr char kCoverageChildEnv[] = "DINGOFS_COVERAGE_CHILD";
}  // namespace

bool CoverageRequested() { return FLAGS_coverage; }

std::string CoverageSourceDir() { return DINGOFS_SOURCE_DIR; }

int RunTestsWithCoverage(const CoverageConfig& config, int argc, char** argv,
                         const std::function<int()>& run_tests) {
  if (!CoverageRequested()) return run_tests();
  if (std::getenv(kCoverageChildEnv) != nullptr) return run_tests();

  const std::string build_dir = CoverageBuildDir();
  if (build_dir.empty()) {
    std::cerr << "coverage: cannot resolve /proc/self/exe\n";
    return run_tests();
  }
  ClearStaleGcda(build_dir);

  // GCC 13 removed the in-process __gcov_dump()/__gcov_flush() API, so
  // counters are only flushed to .gcda when a process exits normally.
  // Re-exec this same binary (same argc/argv) in a child instead of a bare
  // fork(): a plain fork() would be unsafe here since some test binaries
  // link cgo-based clients whose Go runtime owns background OS threads that
  // do not survive a fork, deadlocking the child before it can exit.
  setenv(kCoverageChildEnv, "1", 1);
  std::vector<char*> child_argv(argv, argv + argc);
  child_argv.push_back(nullptr);
  pid_t pid = 0;
  const int spawn_rc = posix_spawn(&pid, argv[0], nullptr, nullptr,
                                    child_argv.data(), environ);
  unsetenv(kCoverageChildEnv);
  if (spawn_rc != 0) {
    std::cerr << "coverage: posix_spawn failed: " << std::strerror(spawn_rc)
               << '\n';
    return run_tests();
  }

  int status = 0;
  if (waitpid(pid, &status, 0) < 0) {
    std::cerr << "coverage: waitpid failed: " << std::strerror(errno) << '\n';
    return 1;
  }
  const int rc = WIFEXITED(status) ? WEXITSTATUS(status) : 1;
  return RunCoverage(config, build_dir, rc);
}

int FinishCoverageReport(const std::string& html, int test_rc) {
  std::cout << "\nHTML report : " << html << '\n';
  return test_rc;
}

std::string BuildGcovrCommand(const CoverageConfig& config,
                              const std::string& repo_root,
                              const std::string& build_dir) {
  const std::string out_dir = build_dir + "/coverage/" + config.target_name;
  std::string excludes;
  for (const auto& exclude : config.excludes) {
    excludes += " --exclude '" + repo_root + "/" + exclude + "'";
  }
  // gcovr resolves a relative --filter against the cwd (not --root), so the
  // source filter must be absolute to work from any directory.
  return "gcovr --root '" + repo_root + "' --filter '" + repo_root + "/" +
         config.source_filter +
         "' --gcov-executable gcov --gcov-ignore-errors=all"
         " --exclude-unreachable-branches --exclude-throw-branches" +
         excludes +
         " --html-details '" +
         out_dir +
         "/index.html'"
         " --txt '" +
         out_dir +
         "/summary.txt'"
         " --csv '" +
         out_dir + "/summary.csv' '" + build_dir + "'";
}

}  // namespace dingofs::unit_test
