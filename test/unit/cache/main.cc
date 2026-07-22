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
 * Created Date: 2026-02-02
 * Author: AI
 */

#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

DECLARE_int32(v);

extern "C" {
// Flushes in-memory coverage counters to .gcda files. Provided by the GCC
// coverage runtime only when built with --coverage; declared weak so the
// normal (non-instrumented) build still links -- there it stays null.
void __gcov_dump(void)
    __attribute__((weak));  // NOLINT(bugprone-reserved-identifier)
}

DEFINE_bool(coverage, false,
            "After tests finish, build a line-coverage report for src/cache "
            "(terminal table + HTML) and serve it over HTTP. Requires a binary "
            "compiled with --coverage; see test/unit/cache/README.md.");

namespace {

// Excluded from the default run, override with --gtest_filter to run
// explicitly: IOUringTest.WaitIO is a known-flaky timing case (200ms upper
// bound flakes under load); SlabAllocatorTest.Perf* are benchmarks (several
// seconds, LOG-only, no assertions) meant to be run on demand.
constexpr char kDefaultFilter[] = "-IOUringTest.WaitIO:SlabAllocatorTest.Perf*";

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

// Binary lives at <build_dir>/bin/test/test_cache.
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

// Bind to port 0 to let the kernel hand us a free ephemeral port, then close.
int PickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  int port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0) {
      port = ntohs(addr.sin_port);
    }
  }
  ::close(fd);
  return port;
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

void PrintTable(const std::vector<FileCov>& rows) {
  const bool tty = ::isatty(STDOUT_FILENO) != 0;
  const char* reset = tty ? "\033[0m" : "";
  const std::string prefix = "src/cache/";

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
  std::cout << '\n' << bar << "\n  src/cache line coverage\n" << dash << '\n';
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

// Run gcovr, print the table, then exec a tiny web server for the HTML report.
// Returns the test exit code only on failure paths (on success it execs away).
int RunCoverage(int test_rc) {
  const std::string build_dir = CoverageBuildDir();
  if (build_dir.empty()) {
    std::cerr << "coverage: cannot resolve /proc/self/exe\n";
    return test_rc != 0 ? test_rc : 1;
  }
  const std::string repo_root = Dirname(build_dir);
  const std::string out_dir = build_dir + "/coverage";
  const std::string html = out_dir + "/index.html";
  const std::string csv = out_dir + "/summary.csv";

  // Counters live in memory and are normally written to .gcda only at exit, but
  // we exec into the web server (no clean exit), so flush them now -- otherwise
  // gcovr would see no data. Null here means the binary lacks --coverage.
  if (__gcov_dump == nullptr) {
    std::cerr << "coverage: binary not built with --coverage; see "
                 "test/unit/cache/README.md\n";
    return test_rc != 0 ? test_rc : 1;
  }
  __gcov_dump();

  if (::mkdir(out_dir.c_str(), 0755) != 0 && errno != EEXIST) {
    std::cerr << "coverage: cannot create " << out_dir << '\n';
    return test_rc != 0 ? test_rc : 1;
  }

  // gcovr resolves a relative --filter against the cwd (not --root), so the
  // src/cache filter must be absolute to work from any directory.
  const std::string cmd =
      "gcovr --root '" + repo_root + "'" + " --filter '" + repo_root +
      "/src/cache/'" + " --gcov-executable gcov --gcov-ignore-errors=all" +
      " --exclude-unreachable-branches --exclude-throw-branches" +
      " --html-details '" + html + "'" + " --txt '" + out_dir +
      "/summary.txt'" + " --csv '" + csv + "'" + " '" + build_dir + "'";

  std::cout << "\n";
  if (std::system(cmd.c_str()) != 0) {  // NOLINT(cert-env33-c)
    std::cerr << "coverage: gcovr failed. Did you build with --coverage and "
                 "run the tests first? See test/unit/cache/README.md\n";
    return test_rc != 0 ? test_rc : 1;
  }

  PrintTable(ParseCsv(csv));

  const int port = PickFreePort();
  if (port == 0) {
    std::cerr << "coverage: no free port; open " << html << " manually\n";
    return test_rc;
  }
  const std::string ports = std::to_string(port);
  std::cout << "\nHTML report : " << html << '\n' << std::flush;

  ::execlp("python3", "python3", "-m", "http.server", ports.c_str(), "--bind",
           "127.0.0.1", "--directory", out_dir.c_str(),
           static_cast<char*>(nullptr));
  std::perror("coverage: failed to launch python3 http.server");
  return test_rc;
}

}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  if (GTEST_FLAG_GET(filter) == "*") {
    GTEST_FLAG_SET(filter, kDefaultFilter);
  }

  // These cache-node flags (defined in cachegroup/) have non-empty validators
  // and the test binary links them. Seed valid values by name so
  // ParseCommandLineFlags does not reject the empty defaults at startup.
  google::SetCommandLineOption("id", "test-cache-node");
  google::SetCommandLineOption("listen_ip", "127.0.0.1");
  google::SetCommandLineOption("group_name", "test-group");

  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_coverage) {
    ClearStaleGcda(CoverageBuildDir());
  }
  int rc = RUN_ALL_TESTS();
  if (FLAGS_coverage) {
    return RunCoverage(rc);
  }
  return rc;
}
