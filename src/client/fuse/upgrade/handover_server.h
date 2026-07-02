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

#ifndef DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SERVER_H_
#define DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SERVER_H_

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include "client/fuse/upgrade/handover_peer.h"

struct fuse_buf;

namespace dingofs {
namespace client {
namespace fuse {

// Old-side handover endpoint: a UDS server that hands the live /dev/fuse fd
// (and the saved INIT message) to a connecting new process, then runs the old
// half of the handover handshake over that same connection. Implements
// HandoverPeer so the handover controller can drive it.
//
// Dependencies are injected so this stays decoupled from FuseServer:
//   - get_dev_fd: returns the /dev/fuse fd to hand off (read at connect time).
//   - init_msg:   the saved INIT message to send alongside the fd; the
//   pointed-to
//                 buffer must outlive this server (owned by FuseServer).
class HandoverServer : public HandoverPeer {
 public:
  HandoverServer(std::string socket_path, std::function<int()> get_dev_fd,
                 const struct fuse_buf* init_msg);
  ~HandoverServer() override;

  HandoverServer(const HandoverServer&) = delete;
  HandoverServer& operator=(const HandoverServer&) = delete;

  // Create+bind+listen the UDS socket and run the accept loop on its own
  // (joinable) thread. No-op on failure (the endpoint is simply unavailable).
  void Start();

  // Stop the accept loop (wake it via the stop eventfd, then join) and close
  // the listen socket. Idempotent. Called by the destructor.
  void Stop();

  // ── HandoverPeer (driven by the handover controller) ──
  bool WaitHandoverPrepare() override;
  bool NotifyReadyToExit() override;
  void NotifyHandoverAbort() override;

 private:
  void AcceptLoop();
  bool IsHandoverInFlight();
  void SetClientFd(int fd);
  void CloseClientFd();

  std::string socket_path_;
  std::function<int()> get_dev_fd_;
  const struct fuse_buf* init_msg_;

  std::thread thread_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_{false};  // set by Stop(), checked by the accept loop
  // Set once the old is past the point of no return (committing the handover).
  // After this, AcceptLoop rejects ALL new connectors -- even when client_fd_
  // is already cleared -- so the /dev/fuse fd is never handed to a second new
  // while this process is winding down.
  std::atomic<bool> committed_{false};
  int server_fd_{-1};      // listen socket (closed by Stop)
  int stop_event_fd_{-1};  // eventfd to wake the accept loop on Stop

  std::mutex mutex_;
  int client_fd_{-1};
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_SERVER_H_
