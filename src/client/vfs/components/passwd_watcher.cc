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

#include "client/vfs/components/passwd_watcher.h"

#include <fmt/format.h>
#include <poll.h>
#include <pthread.h>
#include <sys/eventfd.h>
#include <sys/inotify.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "common/logging.h"

namespace dingofs {
namespace client {
namespace vfs {

namespace {
// A single useradd touches passwd/shadow/group/gshadow in quick succession;
// coalesce the burst so we rebuild the snapshot once, not four times.
constexpr int kDebounceMs = 300;
// Upper bound on a single dirent name (Linux NAME_MAX); sizing the inotify
// read buffer locally avoids pulling in the deprecated <limits.h>.
constexpr size_t kMaxNameLen = 255;
}  // namespace

PasswdWatcher::PasswdWatcher(std::string dir, std::vector<std::string> names,
                             std::function<void()> on_change)
    : dir_(std::move(dir)),
      names_(std::move(names)),
      on_change_(std::move(on_change)) {}

PasswdWatcher::~PasswdWatcher() { Stop(); }

bool PasswdWatcher::Start() {
  inotify_fd_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
  if (inotify_fd_ < 0) {
    LOG(WARNING) << fmt::format("[passwd.watch] inotify_init1 fail: {}",
                                strerror(errno));
    return false;
  }

  // Watch the directory (not the files) so an atomic rename(2) replacement of
  // passwd/group still reaches us as IN_MOVED_TO; IN_CLOSE_WRITE covers an
  // in-place edit.
  if (inotify_add_watch(inotify_fd_, dir_.c_str(),
                        IN_CLOSE_WRITE | IN_MOVED_TO) < 0) {
    LOG(WARNING) << fmt::format("[passwd.watch] add_watch '{}' fail: {}", dir_,
                                strerror(errno));
    close(inotify_fd_);
    inotify_fd_ = -1;
    return false;
  }

  wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    LOG(WARNING) << fmt::format("[passwd.watch] eventfd fail: {}",
                                strerror(errno));
    close(inotify_fd_);
    inotify_fd_ = -1;
    return false;
  }

  stop_.store(false);
  thread_ = std::thread(&PasswdWatcher::Run, this);
  LOG(INFO) << fmt::format("[passwd.watch] watching '{}' for changes.", dir_);
  return true;
}

void PasswdWatcher::Stop() {
  stop_.store(true);

  if (wake_fd_ >= 0) {
    uint64_t one = 1;
    // Best effort — a failed write only matters if the thread is blocked, and
    // stop_ plus a closed fd will still tear it down on the next iteration.
    write(wake_fd_, &one, sizeof(one));
  }

  if (thread_.joinable()) thread_.join();

  if (wake_fd_ >= 0) {
    close(wake_fd_);
    wake_fd_ = -1;
  }
  if (inotify_fd_ >= 0) {
    close(inotify_fd_);
    inotify_fd_ = -1;
  }
}

void PasswdWatcher::Run() {
  // Name the thread so it stands out in top -H / gdb (Linux caps at 15 chars).
  pthread_setname_np(pthread_self(), "passwd-watch");
  LOG(INFO) << fmt::format("[passwd.watch] event loop started, dir '{}'.",
                           dir_);

  // Room for a healthy batch of events; we loop on read() until EAGAIN anyway.
  std::vector<char> buf(64 * (sizeof(struct inotify_event) + kMaxNameLen + 1));
  bool pending = false;

  while (true) {
    struct pollfd fds[2];
    fds[0] = {inotify_fd_, POLLIN, 0};
    fds[1] = {wake_fd_, POLLIN, 0};

    // Block indefinitely when idle; once an event is pending, wait only the
    // debounce window so trailing events of the same burst collapse into one
    // callback when the window elapses.
    int timeout = pending ? kDebounceMs : -1;
    int n = poll(fds, 2, timeout);
    if (n < 0) {
      if (errno == EINTR) continue;
      LOG(ERROR) << fmt::format("[passwd.watch] poll fail: {}",
                                strerror(errno));
      break;
    }

    if (n == 0) {  // debounce window elapsed
      if (pending) {
        on_change_();
        pending = false;
      }
      continue;
    }

    if (fds[1].revents & POLLIN) break;  // Stop() woke us
    if (stop_.load()) break;

    if (fds[0].revents & POLLIN) {
      while (true) {
        ssize_t len = read(inotify_fd_, buf.data(), buf.size());
        if (len <= 0) break;  // EAGAIN (drained) or error — stop reading
        for (char* p = buf.data(); p < buf.data() + len;) {
          auto* ev = reinterpret_cast<struct inotify_event*>(p);
          if (ev->len > 0 && std::find(names_.begin(), names_.end(),
                                       std::string(ev->name)) != names_.end()) {
            pending = true;
          }
          p += sizeof(struct inotify_event) + ev->len;
        }
      }
    }
  }

  LOG(INFO) << fmt::format("[passwd.watch] event loop exited, dir '{}'.", dir_);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
