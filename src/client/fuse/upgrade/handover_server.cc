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

#include "client/fuse/upgrade/handover_server.h"

#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <utility>

#include "client/fuse/fuse_common.h"
#include "client/fuse/upgrade/handover_channel.h"
#include "fuse_lowlevel.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {
namespace fuse {

HandoverServer::HandoverServer(std::string socket_path,
                               std::function<int()> get_dev_fd,
                               const struct fuse_buf* init_msg)
    : socket_path_(std::move(socket_path)),
      get_dev_fd_(std::move(get_dev_fd)),
      init_msg_(init_msg) {}

HandoverServer::~HandoverServer() {
  Stop();
  CloseClientFd();
}

void HandoverServer::Start() {
  if (running_.load()) {
    LOG(INFO) << "dingo-client uds server already started.";
    return;
  }

  // Create + bind + listen synchronously so server_fd_ is valid before the
  // thread and before any Stop(). On any failure the endpoint stays down (no
  // throw): hot-upgrade just won't be available, normal serving continues.
  server_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_fd_ == -1) {
    LOG(ERROR) << "uds server create failed, file: " << socket_path_
               << ", error: " << std::strerror(errno);
    return;
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);
  unlink(socket_path_.c_str());
  if (bind(server_fd_, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
    LOG(ERROR) << "uds server bind failed, file: " << socket_path_
               << ", error: " << std::strerror(errno);
    close(server_fd_);
    server_fd_ = -1;
    return;
  }
  if (listen(server_fd_, 1) == -1) {
    LOG(ERROR) << "uds server listen failed, file: " << socket_path_
               << ", error: " << std::strerror(errno);
    close(server_fd_);
    server_fd_ = -1;
    unlink(socket_path_.c_str());  // bind() created the socket file; remove it
    return;
  }

  stop_event_fd_ = eventfd(0, EFD_CLOEXEC);
  if (stop_event_fd_ == -1) {
    LOG(ERROR) << "uds server eventfd failed, error: " << std::strerror(errno);
    close(server_fd_);
    server_fd_ = -1;
    unlink(socket_path_.c_str());  // bind() created the socket file; remove it
    return;
  }

  stop_.store(false);
  committed_.store(false);
  running_.store(true);
  thread_ = std::thread(&HandoverServer::AcceptLoop, this);

  LOG(INFO) << "dingo-client uds server started on " << socket_path_;
}

void HandoverServer::Stop() {
  if (!running_.load()) return;

  // Wake the accept loop (it polls stop_event_fd_) and join it, then close the
  // listen socket. This must complete before the object is destroyed so the
  // thread never touches freed members.
  stop_.store(true);
  uint64_t one = 1;
  bool woke = stop_event_fd_ >= 0 &&
              write(stop_event_fd_, &one, sizeof(one)) == sizeof(one);
  if (!woke && server_fd_ >= 0) {
    // eventfd write failed; fall back to shutting down the listen socket so
    // poll() returns and the loop sees stop_ -- otherwise join() could hang.
    shutdown(server_fd_, SHUT_RDWR);
  }
  if (thread_.joinable()) thread_.join();

  if (server_fd_ >= 0) {
    close(server_fd_);
    server_fd_ = -1;
  }
  if (stop_event_fd_ >= 0) {
    close(stop_event_fd_);
    stop_event_fd_ = -1;
  }
  running_.store(false);
}

void HandoverServer::SetClientFd(int fd, pid_t pid) {
  // Stores the handover connection, closing any previous one. AcceptLoop
  // rejects a concurrent connector while a LIVE handover is in flight (see
  // IsHandoverInFlight), so any previous fd reached here is only ever a STALE
  // (dead) connection -- closing it is safe and just clears the staleness; it
  // never pulls an fd out from under the controller mid-handshake.
  std::lock_guard<std::mutex> lock(mutex_);
  if (client_fd_ >= 0) {
    close(client_fd_);
  }
  client_fd_ = fd;
  client_pid_ = pid;
}

pid_t HandoverServer::PeerPid() {
  std::lock_guard<std::mutex> lock(mutex_);
  return client_pid_;
}

bool HandoverServer::IsHandoverInFlight() {
  // True if a handover connection is established AND the peer is still alive.
  // Probe with a non-blocking poll under the lock so a concurrent Set/Close
  // cannot pull the fd mid-probe. POLLHUP/POLLERR => peer gone (stale) => not
  // in flight, so a dead connection never blocks a fresh upgrade. A live peer
  // with no event (or buffered kPrepare = POLLIN) counts as in flight.
  std::lock_guard<std::mutex> lock(mutex_);
  if (client_fd_ < 0) return false;

  struct pollfd pfd;
  pfd.fd = client_fd_;
  pfd.events = POLLIN;
  pfd.revents = 0;

  int rc;
  do {
    rc = poll(&pfd, 1, 0);
  } while (rc < 0 && errno == EINTR);

  if (rc < 0) {
    if (errno == EBADF) return false;
    LOG(ERROR) << "hot-upgrade: probe handover fd failed, conservatively "
                  "treat as in-flight, error: "
               << std::strerror(errno);
    return true;
  }

  return (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)) == 0;
}

void HandoverServer::CloseClientFd() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (client_fd_ >= 0) {
    close(client_fd_);
    client_fd_ = -1;
  }
  client_pid_ = -1;
}

bool HandoverServer::WaitHandoverPrepare() {
  int fd = -1;
  pid_t new_pid = -1;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fd = client_fd_;
    new_pid = client_pid_;
  }
  if (fd < 0) {
    LOG(ERROR) << "hot-upgrade: SIGHUP with no handover connection; ignoring";
    return false;
  }

  // The new process sends kPrepare over the UDS before raising SIGHUP, so it is
  // already buffered here. poll() bounds the wait so a dead/closed peer
  // (POLLHUP -> recv EOF) or an alive-but-silent peer (timeout) cannot drive a
  // drain off a stray SIGHUP. poll does not change the fd's blocking mode.
  constexpr int kPrepareTimeoutMs = 5000;
  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLIN;
  pfd.revents = 0;

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(kPrepareTimeoutMs);
  int poll_ret = 0;
  do {
    const auto now = std::chrono::steady_clock::now();
    if (now >= deadline) {
      poll_ret = 0;
      break;
    }
    auto remaining_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)
            .count();
    poll_ret = poll(&pfd, 1, static_cast<int>(remaining_ms));
  } while (poll_ret < 0 && errno == EINTR);

  if (poll_ret <= 0) {
    LOG(ERROR) << "hot-upgrade: no kPrepare from peer (silent/timeout); "
                  "ignoring SIGHUP, keep serving, new_pid: "
               << new_pid;
    CloseClientFd();
    return false;
  }

  HandoverMessage msg;
  if (!RecvHandoverMessage(fd, &msg) || msg != HandoverMessage::kPrepare) {
    LOG(ERROR) << "hot-upgrade: invalid handover prepare; ignoring SIGHUP, "
                  "new_pid: "
               << new_pid;
    CloseClientFd();
    return false;
  }
  return true;
}

bool HandoverServer::NotifyReadyToExit() {
  // Reaching here means the controller is past the checkpoint (VFS torn down)
  // and WILL exit regardless of the notify outcome. Mark committed so the
  // accept loop stops handing the /dev/fuse fd to any further connector,
  // closing the window where client_fd_ is briefly cleared but the process has
  // not exited.
  committed_.store(true);

  int fd = -1;
  pid_t new_pid = -1;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fd = client_fd_;
    new_pid = client_pid_;
  }

  if (fd < 0) {
    // The controller validates the peer via WaitHandoverPrepare() before
    // draining, so reaching here with no fd means the connection was torn down
    // mid-handover.
    LOG(ERROR) << "hot-upgrade: no handover client fd when notifying ready";
    return false;
  }

  if (!SendHandoverMessage(fd, HandoverMessage::kReadyToExit)) {
    LOG(ERROR) << "hot-upgrade: send READY_TO_EXIT failed, new_pid: " << new_pid
               << ", error: " << std::strerror(errno);
    return false;
  }

  LOG(INFO) << "hot-upgrade: READY_TO_EXIT sent to new process, new_pid: "
            << new_pid;
  CloseClientFd();
  return true;
}

void HandoverServer::NotifyHandoverAbort() {
  int fd = -1;
  pid_t new_pid = -1;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fd = client_fd_;
    new_pid = client_pid_;
  }
  if (fd >= 0) {
    if (SendHandoverMessage(fd, HandoverMessage::kNack)) {
      LOG(INFO) << "hot-upgrade: kNack sent to new process, new_pid: "
                << new_pid;
    } else {
      LOG(ERROR) << "hot-upgrade: send kNack to new process failed, new_pid: "
                 << new_pid << ", error: " << std::strerror(errno);
    }
    CloseClientFd();
  }
}

void HandoverServer::AcceptLoop() {
  LOG(INFO) << "uds server listening on " << socket_path_;

  while (true) {
    // Wait for either an incoming connection or a stop request. poll() lets
    // Stop() wake this loop reliably via the eventfd (a bare accept() could not
    // be interrupted cleanly).
    struct pollfd fds[2];
    fds[0].fd = server_fd_;
    fds[0].events = POLLIN;
    fds[0].revents = 0;
    fds[1].fd = stop_event_fd_;
    fds[1].events = POLLIN;
    fds[1].revents = 0;

    int pr = poll(fds, 2, -1);
    if (pr < 0) {
      if (errno == EINTR) continue;
      LOG(ERROR) << "uds server poll failed, error: " << std::strerror(errno);
      break;
    }
    // Stop() may wake us via the eventfd or, as a fallback, by shutting down
    // the listen socket (POLLHUP/POLLERR on server_fd_). Either way, stop_ is
    // set.
    if (stop_.load()) break;
    if (!(fds[0].revents & POLLIN)) continue;

    struct sockaddr_un addr;
    socklen_t addrlen = sizeof(addr);
    int client_fd = accept(server_fd_, (struct sockaddr*)&addr, &addrlen);
    if (client_fd == -1) {
      LOG(ERROR) << "uds server accept failed, error: " << std::strerror(errno);
      continue;
    }

    // Identify the connecting new process for logging; -1 when unavailable.
    pid_t new_pid = -1;
    {
      struct ucred cred;
      socklen_t cred_len = sizeof(cred);
      if (getsockopt(client_fd, SOL_SOCKET, SO_PEERCRED, &cred, &cred_len) ==
          0) {
        new_pid = cred.pid;
      }
    }
    LOG(INFO) << "hot-upgrade: handover connection from new process, new_pid: "
              << new_pid;

    // Once committed, the /dev/fuse fd has been (or is being) handed off
    // exactly once and this process is exiting; reject any further connector
    // regardless of client_fd_ state, so a late new never receives a duplicate
    // fd.
    if (committed_.load()) {
      LOG(WARNING) << "hot-upgrade: handover already committed; rejecting late "
                      "connection, new_pid: "
                   << new_pid;
      close(client_fd);
      continue;
    }

    // Fail-closed against concurrent upgrades: if a handover is already in
    // flight with a live peer, reject this connector instead of replacing it
    // (which would close the fd the controller is mid-handshake on). A stale
    // dead peer is not "in flight", so it does not block a fresh upgrade.
    // Reject BEFORE SendFd so the second new never receives the /dev/fuse fd.
    if (IsHandoverInFlight()) {
      LOG(WARNING) << "hot-upgrade: a handover is already in flight; rejecting "
                      "concurrent upgrade connection (one upgrade per mount), "
                      "new_pid: "
                   << new_pid;
      close(client_fd);
      continue;
    }

    // The INIT message must be saved before it is handed off; otherwise the new
    // process would receive an empty INIT and replay garbage. Start() is only
    // called after SaveOpInitMsg() fills it, so this is belt-and-suspenders.
    if (init_msg_->size == 0) {
      LOG(WARNING) << "hot-upgrade: INIT message not ready yet; rejecting "
                      "handover connection, new_pid: "
                   << new_pid;
      close(client_fd);
      continue;
    }

    // Store the connection BEFORE sending the fd. Once the new receives the fd
    // it can race a kPrepare + SIGHUP straight back, and the controller's
    // WaitHandoverPrepare() must already see client_fd_; otherwise it reads -1
    // and spuriously rejects the handover. On send failure, clear it.
    SetClientFd(client_fd, new_pid);
    int fuse_fd = get_dev_fd_();
    int ret = SendFd(client_fd, fuse_fd, init_msg_->mem, init_msg_->size);
    if (ret == -1) {
      LOG(ERROR) << "hot-upgrade: send fuse fd to new process failed, new_pid: "
                 << new_pid << ", client_fd: " << client_fd;
      CloseClientFd();
    } else {
      LOG(INFO) << "hot-upgrade: fuse fd sent to new process, new_pid: "
                << new_pid << ", fuse_fd: " << fuse_fd
                << ", data size: " << init_msg_->size;
    }
  }
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
