/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_PROFILER_H_
#define DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_PROFILER_H_

#include <sys/types.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "cache/tools/bench/common/flags.h"

namespace dingofs {
namespace cache {
namespace bench {

// Shared --flamegraph options, embedded in every subcommand's Options so all
// subcommands expose the same flags through their own FlagSet (no global gflags).
struct ProfileOptions {
  bool flamegraph{false};      // master switch
  std::string mode{"on_cpu"};  // on_cpu | off_cpu | lock | both | all (comma-ok)
  uint32_t freq{99};           // on-cpu perf sampling frequency (Hz)
  std::string dir;             // output dir; empty => /tmp/cb-flame-<pid>
  uint32_t port{0};            // http port; 0 => auto-pick a free port
};

void RegisterProfileFlags(FlagSet* fs, ProfileOptions* o);
std::string ValidateProfile(const ProfileOptions& o);  // "" = ok

// Wraps the benchmark's measurement window with flame-graph capture:
//   on-cpu  via `perf record` (where CPU time goes),
//   off-cpu via bcc `offcputime` (where threads block / wait on io & locks).
// It fork+execs the samplers attached to cb's own pid, renders interactive SVGs
// with the vendored FlameGraph scripts, then serves them over http and prints a
// URL. Every external tool is optional: a missing one is skipped with a hint,
// never fails the benchmark. When --flamegraph is off the whole class is a no-op.
class Profiler {
 public:
  explicit Profiler(ProfileOptions options) : options_(std::move(options)) {}

  bool enabled() const { return options_.flamegraph; }

  // Begin sampling this process (all threads). No-op unless enabled.
  void Start();
  // Stop sampling and flush capture output (SIGINT + waitpid). No-op if idle.
  void Stop();
  // Render SVGs, write index.html, then exec a web server (does NOT return on
  // success -- the benchmark is already finished). No-op if nothing captured.
  void RenderAndServe();

 private:
  // fork+exec `args`, attached so it dies with us; redirect child stdout/stderr
  // to the given files ("" = inherit). Returns child pid, or -1 on failure.
  pid_t Spawn(const std::vector<std::string>& args, const std::string& out_path,
              const std::string& err_path);

  ProfileOptions options_;

  bool want_on_cpu_{false};
  bool want_off_cpu_{false};
  bool want_lock_{false};
  std::string out_dir_;

  std::string perf_bin_;        // resolved perf, or "" if absent
  std::string offcputime_bin_;  // resolved offcputime, or "" if absent

  pid_t on_cpu_pid_{-1};
  pid_t off_cpu_pid_{-1};
  bool lock_active_{false};  // brpc contention profiler running (in-process)
  bool started_{false};
};

}  // namespace bench
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_BENCH_COMMON_PROFILER_H_
