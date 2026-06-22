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
 * Created Date: 2026-06-22
 * Author: AI
 */

#ifndef DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_PROCESS_GUARD_H_
#define DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_PROCESS_GUARD_H_

#include <sys/types.h>

#include <string>
#include <vector>

namespace dingofs {
namespace cache {
namespace integration {

// Spawns a real binary (e.g. dingo-mds / dingo-cache) as a child process and
// reaps it on destruction. Uses posix_spawn rather than fork()+exec() so it is
// safe to call from this multi-threaded gtest process (fork() with live brpc
// threads leaves the child in an inconsistent state).
class ProcessGuard {
 public:
  ProcessGuard() = default;
  ~ProcessGuard() { Stop(); }

  ProcessGuard(const ProcessGuard&) = delete;
  ProcessGuard& operator=(const ProcessGuard&) = delete;

  // Spawns argv[0] with the given arguments, redirecting the child's stdout and
  // stderr to `log_path`. argv must be non-empty; argv[0] is the binary path.
  // Returns false (and leaves the guard empty) if the spawn fails.
  bool Start(const std::vector<std::string>& argv, const std::string& log_path);

  // True if the child has been started and has not yet exited.
  bool Running() const;

  // Sends SIGTERM, waits up to `term_grace_ms` for a clean exit, then SIGKILL,
  // and finally reaps the child. Idempotent.
  void Stop(int term_grace_ms = 5000);

  pid_t Pid() const { return pid_; }

 private:
  bool Reap(bool block);  // waitpid; sets reaped_ when the child is gone

  pid_t pid_{-1};
  bool reaped_{true};
};

// Grabs a currently-free TCP port by binding 127.0.0.1:0 and reading it back.
// Inherently racy (the port may be taken before the caller binds it), but good
// enough for spinning up a handful of local test servers.
int PickFreePort();

// Polls until a TCP connection to ip:port succeeds, or the timeout elapses.
bool WaitPort(const std::string& ip, int port, int timeout_ms = 20000);

// Directory containing the currently-running executable (via /proc/self/exe).
// Used to locate the dingo-mds / dingo-cache binaries next to the test binary.
std::string ExeDir();

}  // namespace integration
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_INTEGRATION_CACHE_DEPLOY_PROCESS_GUARD_H_
