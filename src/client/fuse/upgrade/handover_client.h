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

#ifndef DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_CLIENT_H_
#define DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_CLIENT_H_

#include <string>

struct fuse_buf;

namespace dingofs {
namespace client {
namespace fuse {

// New-side handover initiator: connects to the old dingo-client's UDS endpoint,
// receives the live /dev/fuse fd + saved INIT message, runs the new half of the
// handshake (kPrepare -> SIGHUP -> wait kReadyToExit -> kAck), and waits for
// the old process to exit. The counterpart of HandoverServer.
class HandoverClient {
 public:
  // Take over the mount served by an old dingo-client. On success returns the
  // received /dev/fuse fd (> 2) and fills init_msg with the old's INIT message;
  // returns -1 on any failure (the caller then aborts the smooth upgrade).
  int TakeOver(const std::string& mountpoint, struct fuse_buf* init_msg);

 private:
  // Poll the mount's stats file until the old dingo-client pid appears (bounded
  // by fuse_fd_get_max_retries). Returns the pid, or <= 0 if not found.
  int FindOldPid(const std::string& stats_file);

  // Connect the old's UDS and receive the /dev/fuse fd + INIT message, retrying
  // until a valid fd arrives or the retry budget runs out. On success returns
  // the fd (> 2) and *comm_fd holds the still-open UDS connection (for the
  // handshake); otherwise returns <= 2.
  int ReceiveFuseFd(const std::string& comm_path, struct fuse_buf* init_msg,
                    int* comm_fd);

  // Run the new half of the handshake on the open UDS connection: announce
  // kPrepare, SIGHUP the old, wait for kReadyToExit, then ACK. Returns false on
  // any failure or a kNack (old rejected and kept serving).
  bool Handshake(int comm_fd, int old_pid);
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_UPGRADE_HANDOVER_CLIENT_H_
