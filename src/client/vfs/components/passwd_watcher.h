// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_VFS_COMPONENTS_PASSWD_WATCHER_H_
#define DINGOFS_SRC_CLIENT_VFS_COMPONENTS_PASSWD_WATCHER_H_

#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace dingofs {
namespace client {
namespace vfs {

// Watches a directory (default /etc) for changes to a set of file names
// (default passwd, group) and fires a debounced callback on change. Lets the
// uid/gid mapper rebuild its snapshot the moment useradd/userdel touches the
// local NSS files, instead of polling on a timer.
//
// We watch the directory rather than the files themselves because useradd and
// vipw replace /etc/passwd via a rename() over the target, which would
// invalidate a watch placed on the file. A directory watch catches both the
// atomic rename (IN_MOVED_TO) and an in-place edit (IN_CLOSE_WRITE).
class PasswdWatcher {
 public:
  PasswdWatcher(std::string dir, std::vector<std::string> names,
                std::function<void()> on_change);

  PasswdWatcher(const PasswdWatcher&) = delete;
  PasswdWatcher& operator=(const PasswdWatcher&) = delete;

  ~PasswdWatcher();

  // Creates the inotify watch and spawns the watch thread. Returns false if
  // inotify setup fails; the caller should log and continue (degraded: the
  // snapshot is still primed once and the outbound miss path covers new ids).
  bool Start();

  // Signals the watch thread to exit, joins it, and closes the fds.
  // Idempotent — safe to call more than once and from the destructor.
  void Stop();

 private:
  void Run();

  const std::string dir_;
  const std::vector<std::string> names_;
  const std::function<void()> on_change_;

  int inotify_fd_{-1};
  int wake_fd_{-1};  // eventfd used to unblock the poll loop on Stop().
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_COMPONENTS_PASSWD_WATCHER_H_
